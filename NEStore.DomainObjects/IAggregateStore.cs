using System;
using System.Threading.Tasks;

namespace NEStore.DomainObjects
{
	public interface IAggregateStore
	{
		Task<T> LoadAsync<T>(Guid objectId) where T : Aggregate;
		Task RebuildAsync();
		Task<WriteResult<IEvent>> SaveAsync(Aggregate aggregate);
	}
}