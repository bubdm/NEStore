using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NEStore.DomainObjects.Events;
using NEStore.DomainObjects.Projections;

namespace NEStore.DomainObjects.Aggregates
{
	public class AggregateStore : IAggregateStore
	{
		public IEventStore<IEvent> EventStore { get; }
		public string BucketName { get; }
		public IBucket<IEvent> Bucket { get; }

		public AggregateStore(IEventStore<IEvent> eventStore, string bucketName)
		{
			EventStore = eventStore;
			BucketName = bucketName;
			Bucket = eventStore.Bucket(bucketName);
		}

		public async Task<WriteResult<IEvent>> SaveAsync(Aggregate aggregate, CancellationToken token = default)
		{
			var lastVersion = aggregate.Version;
			var changes = aggregate.GetChanges()
				.ToList();

			var version = lastVersion - changes.Count;
			var result = await Bucket.WriteAsync(aggregate.ObjectId, version, changes, token)
                    .ConfigureAwait(false);

			aggregate.ClearChanges();

			return result;
		}

		public async Task<T> LoadAsync<T>(Guid objectId, CancellationToken token = default)
			where T : Aggregate
		{
			var events = await Bucket.GetEventsAsync(objectId, token: token)
                            .ConfigureAwait(false);
			var aggregate = (T)Activator.CreateInstance(typeof(T), events);

			return aggregate;
		}

		public async Task RebuildAsync(CancellationToken token = default)
		{
			var projections = EventStore.GetDispatchers()
				.Cast<IProjection>()
				.ToList();

			await Task.WhenAll(projections.Select(p => p.ClearAsync()))
				.ConfigureAwait(false);

			const int bucketsPerPage = 1000;
			var bucketRev = 0L;
			long lastBucketRev;

			do
			{
				lastBucketRev = bucketRev;
				var commits =
					await Bucket.GetCommitsAsync(fromBucketRevision: bucketRev + 1, toBucketRevision: bucketRev + bucketsPerPage, token: token)
						.ConfigureAwait(false);

				foreach (var c in commits)
				{
					await Task.WhenAll(projections.Select(p => p.DispatchAsync(BucketName, c, token)))
						.ConfigureAwait(false);

					bucketRev = c.BucketRevision;
				}
			} while (bucketRev > lastBucketRev);
		}
	}
}
