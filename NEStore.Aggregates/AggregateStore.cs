using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NEStore.Aggregates
{
	public class AggregateStore
	{
		private readonly IBucket<IEvent> _bucket;

		public AggregateStore(IBucket<IEvent> bucket)
		{
			_bucket = bucket;
		}

		public async Task SaveAsync(Aggregate aggregate)
		{
			var lastVersion = aggregate.Version;
			var changes = aggregate.GetChanges()
				.ToList();

			var version = lastVersion - changes.Count;
			await _bucket.WriteAsync(aggregate.ObjectId, version, changes)
                    .ConfigureAwait(false);

			aggregate.ClearChanges();
		}

		public async Task<T> LoadAsync<T>(Guid objectId)
			where T : Aggregate
		{
			var events = (await _bucket.GetEventsAsync(objectId)
                            .ConfigureAwait(false))
				.Cast<IEvent>();
			var aggregate = (T)Activator.CreateInstance(typeof(T), events);

			return aggregate;
		}

		public Task<IEnumerable<IEvent>> GetEventsAsync()
		{
			return _bucket.GetEventsAsync();
		}

		public Task<IEnumerable<CommitData<IEvent>>> GetCommitsAsync()
		{
			return _bucket.GetCommitsAsync();
		}

		public Task RollbackAsync(int bucketRevision)
		{
			return _bucket.RollbackAsync(bucketRevision);
		}
	}
}
