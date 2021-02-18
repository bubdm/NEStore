using System;
using System.Threading;
using System.Threading.Tasks;

namespace NEStore.MongoDb.UndispatchedStrategies
{
	public class DisallowUndispatchedStrategy<T> : IUndispatchedStrategy<T>
	{
		public async Task CheckUndispatchedAsync(IBucket<T> bucket, Guid streamId, CancellationToken token = default)
		{
			var hasUndispatched = await bucket.HasUndispatchedCommitsAsync(token: token)
				.ConfigureAwait(false);

			if (hasUndispatched)
				throw new UndispatchedEventsFoundException("Undispatched events found, cannot write new events");
		}
	}
}
