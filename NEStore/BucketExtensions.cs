using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NEStore
{
	public static class BucketExtensions
	{
		/// <summary>
		/// Writes commits to durable storage and wait for dispatching all events
		/// </summary>
		/// <param name="bucket">Bucket identifier</param>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="expectedStreamRevision">Expected revision of the provided stream</param>
		/// <param name="events">List of events to commit</param>
		/// <param name="token">The Cancellation Token</param>
		/// <returns>WriteResult object containing the commit persisted and the DispatchTask of the events</returns>
		public static async Task<WriteResult<T>> WriteAndDispatchAsync<T>(this IBucket<T> bucket, Guid streamId, int expectedStreamRevision, IEnumerable<T> events, CancellationToken token = default)
		{
			var result = await bucket.WriteAsync(streamId, expectedStreamRevision, events, token)
				.ConfigureAwait(false);

			await result.DispatchTask
				.ConfigureAwait(false);

			return result;
		}

		/// <summary>
		/// Gets the current bucket revision
		/// </summary>
		/// <param name="bucket">Bucket identifier</param>
		/// <param name="token">The Cancellation Token</param>
		/// <returns>Current bucket revision, otherwise 0</returns>
		public static async Task<long> GetBucketRevisionAsync<T>(this IBucket<T> bucket, CancellationToken token = default)
		{
			var lastCommit = await bucket.GetLastCommitAsync(token: token)
				.ConfigureAwait(false);

			return lastCommit?.BucketRevision ?? 0;
		}

		/// <summary>
		/// Gets the current stream revision
		/// </summary>
		/// <param name="bucket">Bucket identifier</param>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="atBucketRevision">Max bucket revision</param>
		/// <param name="token">The Cancellation Token</param>
		/// <returns>Current stream revision, otherwise 0</returns>
		public static async Task<int> GetStreamRevisionAsync<T>(this IBucket<T> bucket, Guid streamId, long? atBucketRevision = null, CancellationToken token = default)
		{
			var lastCommit = await bucket.GetLastCommitAsync(streamId, atBucketRevision, token)
				.ConfigureAwait(false);

			return lastCommit?.StreamRevisionEnd ?? 0;
		}

		/// <summary>
		/// Checks if there are undispatched commits
		/// </summary>
		/// <param name="bucket">Bucket identifier</param>
		/// <param name="streamId">Stream id</param>
		/// <param name="token">The Cancellation Token</param>
		/// <returns>True if there are undispatched commits, otherwise false</returns>
		public static async Task<bool> HasUndispatchedCommitsAsync<T>(this IBucket<T> bucket, Guid? streamId = null, CancellationToken token = default)
		{
			var commits = await bucket.GetCommitsAsync(dispatched: false, streamId: streamId, limit: 1, token: token)
				.ConfigureAwait(false);

			return commits.Any();
		}

		public static async Task<CommitInfo> GetFirstUndispatchedCommitAsync<T>(this IBucket<T> bucket, Guid? streamId = null, CancellationToken token = default)
		{
			var commits = await bucket.GetCommitsAsync(dispatched: false, streamId: streamId, limit: 1, token: token)
				.ConfigureAwait(false);

			return commits.FirstOrDefault();
		}
	}
}