using System;
using System.Threading.Tasks;
using NEStore.DomainObjects.Events;

namespace NEStore.DomainObjects.Aggregates
{
	public interface IAggregateStore
	{
		Task<T> LoadAsync<T>(Guid objectId) where T : Aggregate;
		Task RebuildAsync();
		Task<WriteResult<IEvent>> SaveAsync(Aggregate aggregate);
	}
}