using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NEStore
{
	public static class BucketExtensions
	{
		public static async Task<WriteResult<T>> WriteAndDispatchAsync<T>(this IBucket<T> bucket, Guid streamId, int expectedStreamRevision, IEnumerable<T> events)
		{
			var result = await bucket.WriteAsync(streamId, expectedStreamRevision, events)
				.ConfigureAwait(false);

			await result.DispatchTask
				.ConfigureAwait(false);

			return result;
		}
	}
}