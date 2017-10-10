using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Conventions;
using Moq;
using NEStore.MongoDb.AutoIncrementStrategies;
using NEStore.MongoDb.Conventions;
using NEStore.MongoDb.UndispatchedStrategies;
using Xunit;

namespace NEStore.MongoDb.Tests
{
	public class MongoDbBucketTestsLastCommitStrategy : MongoDbBucketTests
	{
		public override MongoDbEventStoreFixture<T> CreateFixture<T>()
		{
			var f = new MongoDbEventStoreFixture<T>(85);
			f.EventStore.AutonIncrementStrategy = new IncrementFromLastCommitStrategy();
			return f;
		}
	}

	public class MongoDbBucketTestsCountersStrategy : MongoDbBucketTests
	{
		public override MongoDbEventStoreFixture<T> CreateFixture<T>()
		{
			var f = new MongoDbEventStoreFixture<T>(33);
			f.EventStore.AutonIncrementStrategy = new IncrementCountersStrategy<T>(f.EventStore);
			return f;
		}

		// Note: this test only works for IncrementCountersStrategy
		[Fact]
		public async Task Should_dispatch_only_once_when_a_dispatch_is_slow_and_a_new_write_arrive()
		{
			// NOTE: Events are supposed to be idempotent, so they can be eventualy redispatched multiple times,
			//  but I want to ensure that this not happen in a short period of time.
			// The same event can be redispatched in case of a temporary network problem, db problem, ...
			//  but normally the system ensure that an event is dispatched only once, also on heavy load scenario (concurrency)
			// The idea is to ensure this by waiting (AutoDispatchWaitTime) and check if the system is busy doing dispatching.
			// If the system doesn't dispatch any new commit after the AutoDispatchWaitTime then it is "safe" to try to redispatch it

			using (var fixture = CreateFixture<FakeEventWithIndex>())
			{
				fixture.EventStore.UndispatchedStrategy = new UndispatchAllStrategy<FakeEventWithIndex>()
				{
					// Reduce the autodispatch wait time to have a short test
					AutoDispatchWaitTime = TimeSpan.FromMilliseconds(500),
					AutoDispatchCheckInterval = TimeSpan.FromMilliseconds(50)
				};

				var random = new Random();
				var dispatchedEvents = new ConcurrentBag<FakeEventWithIndex>();
				// Simulate a long dispatch time
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<FakeEventWithIndex>>()))
					.Returns<string, CommitData<FakeEventWithIndex>>((s, c) =>
					{
						foreach (var e in c.Events)
							dispatchedEvents.Add(e);

						return Task.Delay(random.Next(300, 400));
					});

				var events = new object[20]
					.Select((p, i) => new FakeEventWithIndex { Index = i + 1 })
					.ToList();

				// 0
				// Add at least one event (first)
				await fixture.Bucket.WriteAsync(Guid.NewGuid(), 0, new[] { new FakeEventWithIndex { Index = 0 } });

				// n
				// Add more events in parallel (note: with this code probably events will be written NOT in order)
				// ReSharper disable once AccessToDisposedClosure
				await events.ForEachAsync(@event => fixture.Bucket.WriteAsync(Guid.NewGuid(), 0, new[] { @event }));

				// n + 1
				// Add a last event and wait dispatch, this is a way to wait that all events are dispatched...
				await fixture.Bucket.WriteAndDispatchAsync(Guid.NewGuid(), 0, new[] { new FakeEventWithIndex { Index = events.Count + 1 } });

				// Ensure to have dispatched only the actual written events
				var expectedEvents = events.Count + 2;
				fixture.Dispatcher.Verify(
					p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<FakeEventWithIndex>>()),
					Times.Exactly(expectedEvents));
				Assert.Equal(expectedEvents, dispatchedEvents.Count);

				// Ensure all events are dispatched
				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
				// Ensure all events are written
				Assert.Equal(expectedEvents, (await fixture.Bucket.GetCommitsAsync()).Count());

				// Ensure dispatch without diplicates
				for (var i = 0; i < expectedEvents; i++)
					Assert.NotNull(dispatchedEvents.SingleOrDefault(e => e.Index == i));
			}
		}

	}

	public abstract class MongoDbBucketTests
	{
		public abstract MongoDbEventStoreFixture<T> CreateFixture<T>();

		[Fact]
		public async Task Query_empty_collections()
		{
			using (var fixture = CreateFixture<object>())
			{
				Assert.Equal(0, (await fixture.Bucket.GetBucketRevisionAsync()));
				Assert.Equal(0, (await fixture.Bucket.GetStreamRevisionAsync(Guid.NewGuid())));
				Assert.Equal(0, (await fixture.Bucket.GetStreamIdsAsync()).Count());
				Assert.Equal(0, (await fixture.Bucket.GetEventsAsync()).Count());
				Assert.Equal(0, (await fixture.Bucket.GetEventsForStreamAsync(Guid.NewGuid())).Count());
				Assert.Equal(0, (await fixture.Bucket.GetCommitsAsync(Guid.NewGuid())).Count());
				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task Write_an_event_and_get_it_back()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { new { n1 = "v1" } });

				Assert.NotNull(result);
				Assert.NotNull(result.Commit);
				Assert.Equal(1, result.Commit.Events.Length);

				Assert.Equal(1, (await fixture.Bucket.GetBucketRevisionAsync()));
				Assert.Equal(streamId, (await fixture.Bucket.GetStreamIdsAsync()).Single());
				Assert.Equal(1, (await fixture.Bucket.GetStreamRevisionAsync(streamId)));

				var storedEvents = await fixture.Bucket.GetEventsAsync(streamId);
				Assert.Equal("v1", ((dynamic)storedEvents.Single()).n1);

				var commits = await fixture.Bucket.GetCommitsAsync(toBucketRevision: 1);
				Assert.Equal(1, commits.Count());

				await result.DispatchTask;
			}
		}

		[Fact]
		public async Task Write_an_event_and_check_for_dispatch()
		{
			using (var fixture = CreateFixture<object>())
			{
				var bucket = fixture.BucketName;
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				await result.DispatchTask;

				fixture.Dispatcher.Verify(p => p.DispatchAsync(bucket, result.Commit), Times.Once());

				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task WriteAndDispatch_an_event_and_check_for_dispatch()
		{
			using (var fixture = CreateFixture<object>())
			{
				var bucket = fixture.BucketName;
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				var result = await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { @event });

				fixture.Dispatcher.Verify(p => p.DispatchAsync(bucket, result.Commit), Times.Once());

				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task Cannot_write_with_revision_less_than_0()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				await
					Assert.ThrowsAsync<ArgumentOutOfRangeException>(
						() => fixture.Bucket.WriteAsync(streamId, -1, new[] { new { n1 = "v2" } }));
			}
		}

		[Fact]
		public async Task Cannot_write_with_revision_not_sequential()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" } });

				await
					Assert.ThrowsAsync<ArgumentOutOfRangeException>(
						() => fixture.Bucket.WriteAsync(streamId, 2, new[] { new { n1 = "v2" } }));
			}
		}

		[Fact]
		public async Task Cannot_write_with_the_same_revision_multiple_times()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" } });

				await
					Assert.ThrowsAsync<ConcurrencyWriteException>(() => fixture.Bucket.WriteAsync(streamId, 0, new[] { new { n1 = "v2" } }));
			}
		}

		[Fact]
		public async Task Cannot_write_with_the_same_revision_multiple_times_using_mongo_index()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				fixture.EventStore.CheckStreamRevisionBeforeWriting = false;

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" } });

				await
					Assert.ThrowsAsync<ConcurrencyWriteException>(() => fixture.Bucket.WriteAsync(streamId, 0, new[] { new { n1 = "v2" } }));
			}
		}

		[Fact]
		public async Task Write_multiple_events()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" }, new { n1 = "v2" }, new { n1 = "v3" } });

				Assert.Equal(1, (await fixture.Bucket.GetBucketRevisionAsync()));
				Assert.Equal(streamId, (await fixture.Bucket.GetStreamIdsAsync()).Single());
				Assert.Equal(3, (await fixture.Bucket.GetStreamRevisionAsync(streamId)));

				var storedEvents = (await fixture.Bucket.GetEventsAsync(streamId)).ToList();
				Assert.Equal("v1", ((dynamic)storedEvents.ElementAt(0)).n1);
				Assert.Equal("v2", ((dynamic)storedEvents.ElementAt(1)).n1);
				Assert.Equal("v3", ((dynamic)storedEvents.ElementAt(2)).n1);

				var commits = await fixture.Bucket.GetCommitsAsync(toBucketRevision: 1);
				Assert.Equal(1, commits.Count());

				var ids = await fixture.Bucket.GetStreamIdsAsync();
				Assert.Equal(streamId, ids.Single());

				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task Write_multiple_commits()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 1, new[] { new { n1 = "v2" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 2, new[] { new { n1 = "v3" } });

				Assert.Equal(3, (await fixture.Bucket.GetBucketRevisionAsync()));
				Assert.Equal(streamId, (await fixture.Bucket.GetStreamIdsAsync()).Single());
				Assert.Equal(3, (await fixture.Bucket.GetStreamRevisionAsync(streamId)));

				var storedEvents = (await fixture.Bucket.GetEventsAsync(streamId)).ToList();
				Assert.Equal("v1", ((dynamic)storedEvents.ElementAt(0)).n1);
				Assert.Equal("v2", ((dynamic)storedEvents.ElementAt(1)).n1);
				Assert.Equal("v3", ((dynamic)storedEvents.ElementAt(2)).n1);

				var commits = await fixture.Bucket.GetCommitsAsync(toBucketRevision: 3);
				Assert.Equal(3, commits.Count());

				var ids = await fixture.Bucket.GetStreamIdsAsync();
				Assert.Equal(streamId, ids.Single());

				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task Write_multiple_commits_with_multiple_events_on_same_stream()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" }, new { n1 = "v2" }, new { n1 = "v3" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 3, new[] { new { n1 = "v4" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 4, new[] { new { n1 = "v5" }, new { n1 = "v6" }, new { n1 = "v7" } });

				Assert.Equal(3, (await fixture.Bucket.GetBucketRevisionAsync()));
				Assert.Equal(streamId, (await fixture.Bucket.GetStreamIdsAsync()).Single());
				Assert.Equal(7, (await fixture.Bucket.GetStreamRevisionAsync(streamId)));

				var storedEvents = (await fixture.Bucket.GetEventsAsync(streamId)).ToList();
				Assert.Equal("v1", ((dynamic)storedEvents.ElementAt(0)).n1);
				Assert.Equal("v2", ((dynamic)storedEvents.ElementAt(1)).n1);
				Assert.Equal("v3", ((dynamic)storedEvents.ElementAt(2)).n1);
				Assert.Equal("v4", ((dynamic)storedEvents.ElementAt(3)).n1);
				Assert.Equal("v5", ((dynamic)storedEvents.ElementAt(4)).n1);
				Assert.Equal("v6", ((dynamic)storedEvents.ElementAt(5)).n1);
				Assert.Equal("v7", ((dynamic)storedEvents.ElementAt(6)).n1);

				var commits = await fixture.Bucket.GetCommitsAsync(toBucketRevision: 1);
				Assert.Equal(1, commits.Count());

				var ids = await fixture.Bucket.GetStreamIdsAsync();
				Assert.Equal(streamId, ids.Single());

				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task Write_multiple_streams()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId1 = Guid.NewGuid();
				var streamId2 = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId1, 0, new[] { new { n1 = "v1" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId2, 0, new[] { new { n1 = "v1" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId1, 1, new[] { new { n1 = "v2" } });

				Assert.Equal(3, (await fixture.Bucket.GetBucketRevisionAsync()));
				var streams = (await fixture.Bucket.GetStreamIdsAsync())
					.ToList();
				Assert.Equal(2, streams.Count);
				Assert.True(streams.Contains(streamId1));
				Assert.True(streams.Contains(streamId2));

				Assert.Equal(2, (await fixture.Bucket.GetStreamRevisionAsync(streamId1)));
				Assert.Equal(1, (await fixture.Bucket.GetStreamRevisionAsync(streamId2)));

				var storedEvents = (await fixture.Bucket.GetEventsAsync(streamId1)).ToList();
				Assert.Equal(2, storedEvents.Count);
				Assert.Equal("v1", ((dynamic)storedEvents.ElementAt(0)).n1);
				Assert.Equal("v2", ((dynamic)storedEvents.ElementAt(1)).n1);

				storedEvents = (await fixture.Bucket.GetEventsAsync(streamId2)).ToList();
				Assert.Equal(1, storedEvents.Count);
				Assert.Equal("v1", ((dynamic)storedEvents.ElementAt(0)).n1);
			}
		}

		[Fact]
		public async Task Rollback()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 1, new[] { new { n1 = "v2" } });

				Assert.Equal(2, (await fixture.Bucket.GetBucketRevisionAsync()));

				await fixture.Bucket.RollbackAsync(1);

				Assert.Equal(1, (await fixture.Bucket.GetBucketRevisionAsync()));

				var storedEvents = (await fixture.Bucket.GetEventsAsync(streamId)).ToList();
				Assert.Equal(1, storedEvents.Count);
				Assert.Equal("v1", ((dynamic)storedEvents.ElementAt(0)).n1);
			}
		}

		[Fact]
		public async Task When_rollback_then_next_commit_should_the_right_bucket_revision()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 1, new[] { new { n1 = "v2" } });

				await fixture.Bucket.RollbackAsync(1);

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 1, new[] { new { n1 = "v3" } });

				var commits = (await fixture.Bucket.GetCommitsAsync(streamId)).ToList();
				Assert.Equal(2, commits.Count);
				Assert.Equal(1, commits.ElementAt(0).BucketRevision);
				Assert.Equal(2, commits.ElementAt(1).BucketRevision);

				Assert.Equal("v1", ((dynamic)commits.ElementAt(0).Events.First()).n1);
				Assert.Equal("v3", ((dynamic)commits.ElementAt(1).Events.First()).n1);
			}
		}

		[Fact]
		public async Task When_rollback_an_empty_bucket_no_error_is_thrown()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.RollbackAsync(0);

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 1, new[] { new { n1 = "v3" } });

				var commits = (await fixture.Bucket.GetCommitsAsync(streamId)).ToList();
				Assert.Equal(1, commits.Count);
				Assert.Equal(1, commits.ElementAt(0).BucketRevision);

				Assert.Equal("v3", ((dynamic)commits.ElementAt(0).Events.First()).n1);
			}
		}

		[Fact]
		public async Task If_dispatch_fail_commits_is_marked_as_undispatched()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()))
					.Throws(new MyException("Some dispatch exception"));

				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				fixture.Dispatcher.Verify(p => p.DispatchAsync(It.IsAny<string>(), result.Commit), Times.Once());

				await Assert.ThrowsAsync<MyException>(() => result.DispatchTask);

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task Undispatch_events_block_write_on_same_bucket()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();
				var @event = new { n1 = "v1" };
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				await Assert.ThrowsAsync<MyException>(() => result.DispatchTask);

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				var streamId2 = Guid.NewGuid();
				var @event2 = new { n1 = "v1" };

				await
					Assert.ThrowsAsync<UndispatchedEventsFoundException>(() => fixture.Bucket.WriteAsync(streamId2, 0, new[] { @event2 }));
			}
		}

		[Fact]
		public async Task Undispatch_events_doesnt_block_write_on_other_buckets()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();
				var @event = new { n1 = "v1" };
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				await Assert.ThrowsAsync<MyException>(() => result.DispatchTask);

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				using (var fixtureBucket2 = CreateFixture<object>())
				{
					var streamId2 = Guid.NewGuid();
					var @event2 = new { n1 = "v1" };

					await fixtureBucket2.Bucket.WriteAndDispatchAsync(streamId2, 0, new[] { @event2 });

					Assert.Equal(false, await fixtureBucket2.Bucket.HasUndispatchedCommitsAsync());
				}
			}
		}


		[Fact]
		public async Task Can_redispatch_undispatched_events()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				// Create an undispatched event
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				await Assert.ThrowsAsync<MyException>(() => result.DispatchTask);

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				// Reset mock
				fixture.Dispatcher.Reset();

				// Redispatch events
				await fixture.Bucket.DispatchUndispatchedAsync();
				fixture.Dispatcher.Verify(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()), Times.Once());
				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task Can_set_undispatched_events_as_dispatched()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				// Create an undispatched event
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				await Assert.ThrowsAsync<MyException>(() => result.DispatchTask);

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				// Reset mock
				fixture.Dispatcher.Reset();

				// Mark as dispatched events
				await fixture.Bucket.SetAllAsDispatched();
				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task Cannot_write_new_event_if_undispatched_events_are_found_and_when_using_DisallowUndispatchedStrategy()
		{
			using (var fixture = CreateFixture<object>())
			{
				fixture.EventStore.UndispatchedStrategy = new DisallowUndispatchedStrategy<object>();

				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				// Create an undispatched event
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				await Assert.ThrowsAsync<MyException>(() => result.DispatchTask);

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				await
					Assert.ThrowsAsync<UndispatchedEventsFoundException>(() => fixture.Bucket.WriteAsync(streamId, 1, new[] { @event }));

				fixture.Dispatcher.Verify(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()), Times.Once());
			}
		}

		[Fact]
		public async Task Can_write_new_event_if_undispatched_events_are_found_and_when_using_IgnoreUndispatchedStrategy()
		{
			using (var fixture = CreateFixture<object>())
			{
				fixture.EventStore.UndispatchedStrategy = new IgnoreUndispatchedStrategy<object>();

				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				// Create an undispatched event
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				await Assert.ThrowsAsync<MyException>(() => result.DispatchTask);

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				await fixture.Bucket.WriteAsync(streamId, 1, new[] { @event });

				fixture.Dispatcher.Verify(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()), Times.Exactly(2));
			}
		}

		[Fact]
		public async Task Should_dispatch_undispatched_events_at_next_write_and_cannot_write_new_event()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				// Create an undispatched event
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				await Assert.ThrowsAsync<MyException>(() => result.DispatchTask);

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				await
					Assert.ThrowsAsync<UndispatchedEventsFoundException>(() => fixture.Bucket.WriteAsync(streamId, 1, new[] { @event }));

				fixture.Dispatcher.Verify(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()), Times.Exactly(2));
			}
		}

		[Fact]
		public async Task Should_dispatch_undispatched_events_at_next_write()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				// Create an undispatched event
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				await Assert.ThrowsAsync<MyException>(() => result.DispatchTask);

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()))
				.Returns<string, CommitData<object>>((b, c) => Task.Delay(50));
				
				await fixture.Bucket.WriteAsync(streamId, 1, new[] { @event });

				fixture.Dispatcher.Verify(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<object>>()), Times.Exactly(3));
			}
		}

		[Fact]
		public async Task Get_all_events_in_a_bucket()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();
				var streamId2 = Guid.NewGuid();
				var streamId3 = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" }, new { n1 = "v2" }, new { n1 = "v3" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId2, 0, new[] { new { n1 = "v4" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId3, 0, new[] { new { n1 = "v5" }, new { n1 = "v6" }, new { n1 = "v7" } });

				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());

				var allEvents = (await fixture.Bucket.GetEventsAsync())
					.ToList();

				Assert.Equal(7, allEvents.Count());

				Assert.Equal("v1", ((dynamic)allEvents.ElementAt(0)).n1);
				Assert.Equal("v2", ((dynamic)allEvents.ElementAt(1)).n1);
				Assert.Equal("v3", ((dynamic)allEvents.ElementAt(2)).n1);
				Assert.Equal("v4", ((dynamic)allEvents.ElementAt(3)).n1);
				Assert.Equal("v5", ((dynamic)allEvents.ElementAt(4)).n1);
				Assert.Equal("v6", ((dynamic)allEvents.ElementAt(5)).n1);
				Assert.Equal("v7", ((dynamic)allEvents.ElementAt(6)).n1);
			}
		}

		[Fact]
		public async Task It_should_not_return_events_with_invalid_arguments()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" }, new { n1 = "v2" }, new { n1 = "v3" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 3, new[] { new { n1 = "v4" } });
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 4, new[] { new { n1 = "v5" }, new { n1 = "v6" }, new { n1 = "v7" } });

				await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => fixture.Bucket.GetEventsAsync(fromBucketRevision: 0));
				await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => fixture.Bucket.GetEventsAsync(fromBucketRevision: -1));
				await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => fixture.Bucket.GetEventsAsync(toBucketRevision: 0));
				await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => fixture.Bucket.GetEventsAsync(toBucketRevision: -1));
			}
		}

		[Fact]
		public async Task It_should_not_return_events_for_stream_with_invalid_arguments()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				var e1 = new { n1 = "v1" };
				var e2 = new { n1 = "v2" };
				var e3 = new { n1 = "v3" };

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { e1, e2, e3 });

				await
					Assert.ThrowsAsync<ArgumentOutOfRangeException>(
						() => fixture.Bucket.GetEventsForStreamAsync(streamId, fromStreamRevision: 0));
				await
					Assert.ThrowsAsync<ArgumentOutOfRangeException>(
						() => fixture.Bucket.GetEventsForStreamAsync(streamId, fromStreamRevision: -1));
				await
					Assert.ThrowsAsync<ArgumentOutOfRangeException>(
						() => fixture.Bucket.GetEventsForStreamAsync(streamId, toStreamRevision: 0));
				await
					Assert.ThrowsAsync<ArgumentOutOfRangeException>(
						() => fixture.Bucket.GetEventsForStreamAsync(streamId, toStreamRevision: -1));
			}
		}

		[Fact]
		public async Task Get_events_for_stream()
		{
			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();

				var e1 = new FakeEvent(1);
				var e2 = new FakeEvent(2);
				var e3 = new FakeEvent(3);
				var e4 = new FakeEvent(4);
				var e5 = new FakeEvent(5);
				var e6 = new FakeEvent(6);
				var e7 = new FakeEvent(7);

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { e1, e2, e3 });
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 3, new[] { e4 });
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 4, new[] { e5, e6, e7 });

				var allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId));
				Assert.Equal(new[] { e1, e2, e3, e4, e5, e6, e7 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 1, 7));
				Assert.Equal(new[] { e1, e2, e3, e4, e5, e6, e7 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, toStreamRevision: 8465));
				Assert.Equal(new[] { e1, e2, e3, e4, e5, e6, e7 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId));
				Assert.Equal(new[] { e1, e2, e3, e4, e5, e6, e7 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 1, 8465));
				Assert.Equal(new[] { e1, e2, e3, e4, e5, e6, e7 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 1, 3));
				Assert.Equal(new[] { e1, e2, e3 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 1, 4));
				Assert.Equal(new[] { e1, e2, e3, e4 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 1, 2));
				Assert.Equal(new[] { e1, e2 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 2, 2));
				Assert.Equal(new[] { e2 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 7, 7));
				Assert.Equal(new[] { e7 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 7, 8));
				Assert.Equal(new[] { e7 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 7, 999));
				Assert.Equal(new[] { e7 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 1, 5));
				Assert.Equal(new[] { e1, e2, e3, e4, e5 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 2, 5));
				Assert.Equal(new[] { e2, e3, e4, e5 }.ToList<object>(), allEvents);

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 8, 8));
				Assert.Equal(0, allEvents.Count());

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, 8, 99));
				Assert.Equal(0, allEvents.Count());
			}
		}

		[Fact]
		public async Task Get_events_with_pagination_from_bucket()
		{
			using (var fixture = CreateFixture<object>())
			{
				for (var i = 1; i <= 20; i++)
				{
					var streamId = Guid.NewGuid();
					await
						fixture.Bucket.WriteAndDispatchAsync(streamId, 0,
							new[] { new { n1 = $"v{i}.1" }, new { n1 = $"v{i}.2" } });
				}

				var allEvents = await fixture.Bucket.GetEventsAsync();
				Assert.Equal(40, allEvents.Count());

				var pagedEvents = (await fixture.Bucket.GetEventsAsync(fromBucketRevision: 1, toBucketRevision: 1)).ToList();
				Assert.Equal(2, pagedEvents.Count);
				Assert.Equal("v1.1", ((dynamic)pagedEvents.First()).n1);
				Assert.Equal("v1.2", ((dynamic)pagedEvents.Last()).n1);

				pagedEvents = (await fixture.Bucket.GetEventsAsync(fromBucketRevision: 1, toBucketRevision: 2)).ToList();
				Assert.Equal(4, pagedEvents.Count);
				Assert.Equal("v1.1", ((dynamic)pagedEvents.First()).n1);
				Assert.Equal("v2.2", ((dynamic)pagedEvents.Last()).n1);

				pagedEvents = (await fixture.Bucket.GetEventsAsync(fromBucketRevision: 2, toBucketRevision: 2)).ToList();
				Assert.Equal(2, pagedEvents.Count);
				Assert.Equal("v2.1", ((dynamic)pagedEvents.First()).n1);
				Assert.Equal("v2.2", ((dynamic)pagedEvents.Last()).n1);

				pagedEvents = (await fixture.Bucket.GetEventsAsync(fromBucketRevision: 20, toBucketRevision: 20)).ToList();
				Assert.Equal(2, pagedEvents.Count());
				Assert.Equal("v20.1", ((dynamic)pagedEvents.First()).n1);
				Assert.Equal("v20.2", ((dynamic)pagedEvents.Last()).n1);

				pagedEvents = (await fixture.Bucket.GetEventsAsync(fromBucketRevision: 20, toBucketRevision: 999)).ToList();
				Assert.Equal(2, pagedEvents.Count());
				Assert.Equal("v20.1", ((dynamic)pagedEvents.First()).n1);
				Assert.Equal("v20.2", ((dynamic)pagedEvents.Last()).n1);

				pagedEvents = (await fixture.Bucket.GetEventsAsync(fromBucketRevision: 10, toBucketRevision: 15)).ToList();
				Assert.Equal(12, pagedEvents.Count());
				Assert.Equal("v10.1", ((dynamic)pagedEvents.First()).n1);
				Assert.Equal("v15.2", ((dynamic)pagedEvents.Last()).n1);

				pagedEvents = (await fixture.Bucket.GetEventsAsync(fromBucketRevision: 10, toBucketRevision: 30)).ToList();
				Assert.Equal(22, pagedEvents.Count());
				Assert.Equal("v10.1", ((dynamic)pagedEvents.First()).n1);
				Assert.Equal("v20.2", ((dynamic)pagedEvents.Last()).n1);
			}
		}

		[Fact]
		public async Task Get_commits_with_pagination_from_bucket()
		{
			using (var fixture = CreateFixture<object>())
			{
				for (var i = 1; i <= 20; i++)
				{
					var streamId = Guid.NewGuid();
					await
						fixture.Bucket.WriteAndDispatchAsync(streamId, 0,
							new[] { new { n1 = $"v{i}.1" }, new { n1 = $"v{i}.2" } });
				}

				var allCommits = await fixture.Bucket.GetCommitsAsync();
				Assert.Equal(20, allCommits.Count());

				var pagedCommits = (await fixture.Bucket.GetCommitsAsync(fromBucketRevision: 1, toBucketRevision: 1)).ToList();
				Assert.Equal(1, pagedCommits.Count);
				Assert.Equal(1, pagedCommits.ElementAt(0).BucketRevision);

				pagedCommits = (await fixture.Bucket.GetCommitsAsync(fromBucketRevision: 1, toBucketRevision: 2)).ToList();
				Assert.Equal(2, pagedCommits.Count);
				Assert.Equal(1, pagedCommits.ElementAt(0).BucketRevision);
				Assert.Equal(2, pagedCommits.ElementAt(1).BucketRevision);

				pagedCommits = (await fixture.Bucket.GetCommitsAsync(fromBucketRevision: 20, toBucketRevision: 20)).ToList();
				Assert.Equal(1, pagedCommits.Count);
				Assert.Equal(20, pagedCommits.ElementAt(0).BucketRevision);

				pagedCommits = (await fixture.Bucket.GetCommitsAsync(fromBucketRevision: 21, toBucketRevision: 999)).ToList();
				Assert.Equal(0, pagedCommits.Count);
			}
		}

		[Fact]
		public async Task Dictonary_with_non_string_keys_should_be_serialized_without_BsonSerializationException()
		{
			ConventionRegistry.Register(
				nameof(SafeDictionaryKeyConvention),
				new ConventionPack { new SafeDictionaryKeyConvention() },
				_ => true);

			MongoDbSerialization.Register(typeof(EventWithDictionary<DateTime>));
			MongoDbSerialization.Register(typeof(EventWithDictionary<int>));

			using (var fixture = CreateFixture<object>())
			{
				var streamId = Guid.NewGuid();
				var @event = new EventWithDictionary<DateTime>();
				@event.Add(DateTime.Now, 2);
				@event.Add(DateTime.Today, "test");
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { @event });
				var events = await fixture.Bucket.GetEventsAsync(streamId);
				Assert.Equal("test", ((EventWithDictionary<DateTime>)events.ElementAt(0)).ToBeSerialized[DateTime.Today.ToUniversalTime()]); // why I need this conversion?


				var streamId2 = Guid.NewGuid();
				var @event2 = new EventWithDictionary<int>();
				@event2.Add(100, 2);
				@event2.Add(200, "test");
				await fixture.Bucket.WriteAndDispatchAsync(streamId2, 0, new[] { @event2 });
				var events2 = await fixture.Bucket.GetEventsAsync(streamId2);
				Assert.Equal("test", ((EventWithDictionary<int>)events2.ElementAt(0)).ToBeSerialized[200]);
			}
		}

		[Fact]
		public async Task Events_with_readonly_properties_are_serialized()
		{
			ConventionRegistry.Register(
				nameof(ImmutablePocoConvention),
				new ConventionPack { new ImmutablePocoConvention() },
				_ => true);

			using (var fixture = CreateFixture<EventWithReadOnlyProperties>())
			{
				var streamId = Guid.NewGuid();
				var @event = new EventWithReadOnlyProperties("test1");
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { @event });

				var events = await fixture.Bucket.GetEventsAsync(streamId);
				Assert.Equal("test1", events.ElementAt(0).Name);
			}
		}

		[Fact]
		public async Task Events_with_readonly_properties_and_multiple_constructors_are_serialized()
		{
			ConventionRegistry.Register(
			nameof(ImmutablePocoConvention),
			new ConventionPack { new ImmutablePocoConvention() },
			_ => true);

			using (var fixture = CreateFixture<ImmutablePocoSample>())
			{
				var streamId = Guid.NewGuid();
				var @event1 = new ImmutablePocoSample("Icardi");
				var @event2 = new ImmutablePocoSample("Davide", "Icardi");
				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { @event1, @event2 });

				var events = (await fixture.Bucket.GetEventsAsync(streamId)).ToList();
				Assert.Equal(null, events.ElementAt(0).FirstName);
				Assert.Equal("Icardi", events.ElementAt(0).LastName);
				Assert.Equal("Davide", events.ElementAt(1).FirstName);
				Assert.Equal("Icardi", events.ElementAt(1).LastName);
			}
		}

		[Serializable]
		private class MyException : Exception
		{
			//
			// For guidelines regarding the creation of new exception types, see
			//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
			// and
			//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
			//

			public MyException(string message) : base(message)
			{
			}

			protected MyException(
				SerializationInfo info,
				StreamingContext context) : base(info, context)
			{
			}
		}

		public class FakeEvent
		{
			private readonly int _hashCode;
			public int V { get; private set; }

			public FakeEvent(int v)
			{
				_hashCode = v.GetHashCode();
				V = v;
			}

			public override int GetHashCode()
			{
				return _hashCode.GetHashCode();
			}

			public override bool Equals(object obj)
			{
				if (obj == null || GetType() != obj.GetType())
					return false;

				return V == ((FakeEvent)obj).V;
			}
		}

		public class FakeEventWithIndex
		{
			public int Index { get; set; }
		}

		internal class EventWithDictionary<T>
		{
			public IDictionary<T, object> ToBeSerialized { get; set; }

			public EventWithDictionary()
			{
				ToBeSerialized = new Dictionary<T, object>();
			}

			public void Add(T key, object value)
			{
				ToBeSerialized.Add(key, value);
			}
		}

		public class EventWithReadOnlyProperties
		{
			public string Name { get; }

			public EventWithReadOnlyProperties(string name)
			{
				Name = name;
			}
		}

		public class ImmutablePocoSample
		{
			public string FirstName { get; }
			public string LastName { get; }
			public string FullName => FirstName + LastName;

			public ImmutablePocoSample(string lastName)
			{
				LastName = lastName;
			}

			public ImmutablePocoSample(string firstName, string lastName)
			{
				FirstName = firstName;
				LastName = lastName;
			}
		}
	}

	public static class ParallelExtensions
	{
		public static Task ForEachAsync<T>(this IEnumerable<T> source, Func<T, Task> body)
		{
			return Task.WhenAll(
					from item in source
					select Task.Run(() => body(item)));
		}
	}
}