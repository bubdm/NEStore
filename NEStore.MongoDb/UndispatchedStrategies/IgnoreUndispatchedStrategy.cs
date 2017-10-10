using System;
using System.Threading.Tasks;

namespace NEStore.MongoDb.UndispatchedStrategies
{
	public class IgnoreUndispatchedStrategy<T> : IUndispatchedStrategy<T>
	{
		public Task CheckUndispatchedAsync(IBucket<T> bucket, Guid streamId)
		{
			return Task.FromResult(false);
		}
	}
}
