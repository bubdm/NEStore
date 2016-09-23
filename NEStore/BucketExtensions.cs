using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NEStore
{
	public static class BucketExtensions
	{
		/// <summary>
		/// Writes commits to durable storage and wait for dispatching all events
		/// </summary>
		/// <param name="bucket">The bucket</param>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="expectedStreamRevision">Expected revion of the provided stream</param>
		/// <param name="events">List of events to commit</param>
		/// <returns>WriteResult object containing the commit persisted and the DispatchTask of the events</returns>
		public static async Task<WriteResult<T>> WriteAndDispatchAsync<T>(this IBucket<T> bucket, Guid streamId, int expectedStreamRevision, IEnumerable<T> events)
		{
			var result = await bucket.WriteAsync(streamId, expectedStreamRevision, events)
				.ConfigureAwait(false);

			await result.DispatchTask
				.ConfigureAwait(false);

			return result;
		}

		/// <summary>
		/// Get the current bucket revision
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="bucket"></param>
		/// <returns></returns>
		public static async Task<long> GetBucketRevisionAsync<T>(this IBucket<T> bucket)
		{
			var lastCommit = await bucket.GetLastCommit();

			return lastCommit?.BucketRevision ?? 0;
		}

		/// <summary>
		/// Get the current stream revision
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="bucket"></param>
		/// <param name="streamId"></param>
		/// <param name="atBucketRevision"></param>
		/// <returns></returns>
		public static async Task<int> GetStreamRevisionAsync<T>(this IBucket<T> bucket, Guid streamId, long? atBucketRevision = null)
		{
			var lastCommit = await bucket.GetLastCommit(streamId, atBucketRevision);

			return lastCommit?.StreamRevisionEnd ?? 0;
		}

		/// <summary>
		/// Returns true if there are undispatched commits
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="bucket"></param>
		/// <returns></returns>
		public static async Task<bool> HasUndispatchedCommitsAsync<T>(this IBucket<T> bucket)
		{
			var lastCommit = await bucket.GetLastCommit();

			if (lastCommit == null)
				return false;

			return !lastCommit.Dispatched;
		}
	}
}