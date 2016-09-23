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
	}
}