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

		public static async Task<long> GetBucketRevisionAsync<T>(this IBucket<T> bucket)
		{
			var lastCommit = await bucket.GetLastCommit();

			return lastCommit?.BucketRevision ?? 0;
		}

		public static async Task<int> GetStreamRevisionAsync<T>(this IBucket<T> bucket, Guid streamId, long? atBucketRevision = null)
		{
			var lastCommit = await bucket.GetLastCommit(streamId, atBucketRevision);

			return lastCommit?.StreamRevisionEnd ?? 0;
		}

		public static async Task<bool> HasUndispatchedCommitsAsync<T>(this IBucket<T> bucket)
		{
			var lastCommit = await bucket.GetLastCommit();

			if (lastCommit == null)
				return false;

			return !lastCommit.Dispatched;
		}
	}
}