using System;
using System.Threading.Tasks;

namespace NEStore.MongoDb.UndispatchedStrategies
{
	public class DisallowUndispatchedStrategy<T> : IUndispatchedStrategy<T>
	{
		public async Task CheckUndispatchedAsync(IBucket<T> bucket, Guid streamId)
		{
			var hasUndispatched = await bucket.HasUndispatchedCommitsAsync()
				.ConfigureAwait(false);

			if (hasUndispatched)
				throw new UndispatchedEventsFoundException("Undispatched events found, cannot write new events");
		}
	}
}
