using System;
using System.Threading;
using System.Threading.Tasks;
using NEStore.DomainObjects.Events;

namespace NEStore.DomainObjects.Aggregates
{
	public interface IAggregateStore
	{
		Task<T> LoadAsync<T>(Guid objectId, CancellationToken token = default) where T : Aggregate;
		Task RebuildAsync(CancellationToken token = default);
		Task<WriteResult<IEvent>> SaveAsync(Aggregate aggregate, CancellationToken token = default);
	}
}