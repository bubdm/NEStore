using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace StreamLedger.Aggregates
{
	public class AggregateStore
	{
		private readonly IBucket _bucket;

		public AggregateStore(IBucket bucket)
		{
			_bucket = bucket;
		}

		public async Task SaveAsync(Aggregate aggregate)
		{
			var lastVersion = aggregate.Version;
			var changes = aggregate.GetChanges()
				.ToList();

			var version = lastVersion - changes.Count;
			await _bucket.WriteAsync(aggregate.ObjectId, version, changes);

			aggregate.ClearChanges();
		}

		public async Task<T> LoadAsync<T>(Guid objectId)
			where T : Aggregate
		{
			var events = (await _bucket.GetEventsAsync(objectId))
				.Cast<IEvent>();
			var aggregate = (T)Activator.CreateInstance(typeof(T), events);

			return aggregate;
		}

		public Task<IEnumerable<object>> GetEventsAsync()
		{
			return _bucket.GetEventsAsync();
		}

		public Task RollbackAsync(int bucketRevision)
		{
			return _bucket.RollbackAsync(bucketRevision);
		}
	}
}
