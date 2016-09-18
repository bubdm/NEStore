using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Moq;
using Xunit;

namespace NEStore.MongoDb.Tests
{
	public class MongoDbBucketTests
	{
		[Fact]
		public async Task Query_empty_collections()
		{
			using (var fixture = new MongoDbEventStoreFixture())
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
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.WriteAsync(streamId, 0, new[] { new { n1 = "v1" } });

				Assert.Equal(1, (await fixture.Bucket.GetBucketRevisionAsync()));
				Assert.Equal(streamId, (await fixture.Bucket.GetStreamIdsAsync()).Single());
				Assert.Equal(1, (await fixture.Bucket.GetStreamRevisionAsync(streamId)));

				var storedEvents = await fixture.Bucket.GetEventsAsync(streamId);
				Assert.Equal("v1", ((dynamic)storedEvents.Single()).n1);

				var commits = await fixture.Bucket.GetCommitsAsync(toBucketRevision: 1);
				Assert.Equal(1, commits.Count());
			}
		}

		[Fact]
		public async Task Write_an_event_and_check_for_dispatch()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				await result.DispatchTask;

				fixture.Dispatcher.Verify(p => p.DispatchAsync(@event), Times.Once());

				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task WriteAndDispatch_an_event_and_check_for_dispatch()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { @event });

				fixture.Dispatcher.Verify(p => p.DispatchAsync(@event), Times.Once());

				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task Cannot_write_with_revision_less_than_0()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();

				await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => fixture.Bucket.WriteAsync(streamId, -1, new[] { new { n1 = "v2" } }));
			}
		}

		[Fact]
		public async Task Cannot_write_with_revision_not_sequential()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" } });

				await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => fixture.Bucket.WriteAsync(streamId, 2, new[] { new { n1 = "v2" } }));
			}
		}

		[Fact]
		public async Task Cannot_write_with_the_same_revision_multiple_times()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" } });

				await Assert.ThrowsAsync<ConcurrencyWriteException>(() => fixture.Bucket.WriteAsync(streamId, 0, new[] { new { n1 = "v2" } }));
			}
		}

		[Fact]
		public async Task Cannot_write_with_the_same_revision_multiple_times_using_mongo_index()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();

				fixture.EventStore.CheckStreamRevisionBeforeWriting = false;

				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, new[] { new { n1 = "v1" } });

				await Assert.ThrowsAsync<ConcurrencyWriteException>(() => fixture.Bucket.WriteAsync(streamId, 0, new[] { new { n1 = "v2" } }));
			}
		}

		[Fact]
		public async Task Write_multiple_events()
		{
			using (var fixture = new MongoDbEventStoreFixture())
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
			using (var fixture = new MongoDbEventStoreFixture())
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
			using (var fixture = new MongoDbEventStoreFixture())
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
			using (var fixture = new MongoDbEventStoreFixture())
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
			using (var fixture = new MongoDbEventStoreFixture())
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
			using (var fixture = new MongoDbEventStoreFixture())
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
		public async Task If_dispatch_fail_commits_is_marked_as_undispatched()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<object>()))
					.Throws(new MyException("Some dispatch exception"));

				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				fixture.Dispatcher.Verify(p => p.DispatchAsync(@event), Times.Once());

				try
				{
					await result.DispatchTask;
				}
				catch (MyException)
				{ }

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task Undispatch_events_block_write_on_same_bucket()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();
				var @event = new { n1 = "v1" };
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<object>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				try
				{
					await result.DispatchTask;
				}
				catch (MyException)
				{ }

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				var streamId2 = Guid.NewGuid();
				var @event2 = new { n1 = "v1" };

				await Assert.ThrowsAsync<UndispatchedEventsFoundException>(() => fixture.Bucket.WriteAsync(streamId2, 0, new[] { @event2 }));
			}
		}

		[Fact]
		public async Task Undispatch_events_doesnt_block_write_on_other_buckets()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();
				var @event = new { n1 = "v1" };
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<object>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });

				try
				{
					await result.DispatchTask;
				}
				catch (MyException)
				{ }

				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				using (var fixtureBucket2 = new MongoDbEventStoreFixture())
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
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				// Create an undispatched event
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<object>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });
				try
				{
					await result.DispatchTask;
				}
				catch (MyException)
				{ }
				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				// Reset mock
				fixture.Dispatcher.Reset();

				// Redispatch events
				await fixture.Bucket.DispatchUndispatchedAsync();
				fixture.Dispatcher.Verify(p => p.DispatchAsync(It.IsAny<object>()), Times.Once());
				Assert.Equal(false, await fixture.Bucket.HasUndispatchedCommitsAsync());
			}
		}

		[Fact]
		public async Task Cannot_write_new_event_if_there_are_undispatched_events()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				var streamId = Guid.NewGuid();

				var @event = new { n1 = "v1" };

				// Create an undispatched event
				fixture.Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<object>()))
					.Throws(new MyException("Some dispatch exception"));
				var result = await fixture.Bucket.WriteAsync(streamId, 0, new[] { @event });
				try
				{
					await result.DispatchTask;
				}
				catch (MyException)
				{ }
				Assert.Equal(true, await fixture.Bucket.HasUndispatchedCommitsAsync());

				await Assert.ThrowsAsync<UndispatchedEventsFoundException>(() => fixture.Bucket.WriteAsync(streamId, 1, new[] { @event }));
			}
		}

		[Fact]
		public async Task Get_all_events_in_a_bucket()
		{
			using (var fixture = new MongoDbEventStoreFixture())
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
			using (var fixture = new MongoDbEventStoreFixture())
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
			using (var fixture = new MongoDbEventStoreFixture())
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
			using (var fixture = new MongoDbEventStoreFixture())
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

				allEvents = (await fixture.Bucket.GetEventsForStreamAsync(streamId, fromStreamRevision: 1));
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
			using (var fixture = new MongoDbEventStoreFixture())
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
			using (var fixture = new MongoDbEventStoreFixture())
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
	}
}