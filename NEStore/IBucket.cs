using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NEStore
{
	public interface IBucket<T>
	{
		/// <summary>
		/// Gets the value which identifies bucket
		/// </summary>
		string BucketName { get; }

		/// <summary>
		/// Persist a commit to durable storage
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="expectedStreamRevision">Expected revision of the provided stream</param>
		/// <param name="events">List of events to commit</param>
		/// <param name="token">The Cancellation Token</param>
		/// <returns>WriteResult object containing the commit persisted and the DispatchTask of the events</returns>
		Task<WriteResult<T>> WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<T> events, CancellationToken token = default);

		/// <summary>
		/// Dispatch all commits where dispatched attribute is set to false
		/// </summary>
		Task<CommitData<T>[]> DispatchUndispatchedAsync(Guid? streamId, long? toBucketRevision, CancellationToken token = default);

		/// <summary>
		/// Set all undispatched events as dispatched, without dispatching them
		/// </summary>
		Task SetAllAsDispatched(CancellationToken token = default);

		/// <summary>
		/// Delete all commits succeeding the revision provided
		/// </summary>
		/// <param name="bucketRevision">Revision of last commit to keep</param>
		/// <param name="token">The Cancellation Token</param>
		Task RollbackAsync(long bucketRevision, CancellationToken token = default);

		/// <summary>
		/// Retrieve all events from bucket filtered by params
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="fromBucketRevision">Start bucket revision</param>
		/// <param name="toBucketRevision">End bucket revision</param>
		/// <param name="token">The Cancellation Token</param>
		/// <returns>Flattered list of events retrieved from commits</returns>
		Task<IEnumerable<T>> GetEventsAsync(Guid? streamId = null, long fromBucketRevision = 1, long? toBucketRevision = null, CancellationToken token = default);

		/// <summary>
		/// Retrieve all events from bucket filtered by params
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="fromStreamRevision">
		/// Start stream revision. This point is included in the performed search.
		/// </param>
		/// <param name="toStreamRevision">
		/// End stream revision. This point is included in the performed search.
		/// </param>
		/// <param name="token">The Cancellation Token</param>
		/// <returns>
		/// Flattered list of events retrieved from commits
		/// </returns>
		/// <remarks>
		/// This method is meant to return the event which transition the aggregate 
		/// to revision fromStreamRevision, the event which transition the aggregate to
		/// revision fromStreamRevision + 1, the event which transition the aggregate to
		/// revision fromStreamRevision + 2, ..., the event which transition the aggregate to
		/// revision toStreamRevision. Both the ends (fromStreamRevision and toStreamRevision) are included.
		/// </remarks>
		Task<IEnumerable<T>> GetEventsForStreamAsync(Guid streamId, int fromStreamRevision = 1, int? toStreamRevision = null, CancellationToken token = default);

		/// <summary>
		/// Retrieve all commits from bucket filtered by params. Ordered by bucket revision.
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="fromBucketRevision">Start bucket revision</param>
		/// <param name="toBucketRevision">End bucket revision</param>
		/// <param name="dispatched">Include/exclude dispatched</param>
		/// <param name="limit">Number of items returned</param>
		/// <param name="token">The Cancellation Token</param>
		/// <returns>List of commits matching filters</returns>
		Task<IEnumerable<CommitData<T>>> GetCommitsAsync(
			Guid? streamId = null,
			long fromBucketRevision = 1,
			long? toBucketRevision = null,
			bool? dispatched = null,
			int? limit = null,
			CancellationToken token = default);

		/// <summary>
		/// Retrieve the latest commit matching the specified criteria
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="atBucketRevision">Get the last commit less or equal the specified bucket revision</param>
		/// <param name="token">The Cancellation Token</param>
		/// <returns>Last commit info</returns>
		Task<CommitInfo> GetLastCommitAsync(Guid? streamId = null, long? atBucketRevision = null, CancellationToken token = default);

		/// <summary>
		/// Retrieve all streams inside the range provided
		/// </summary>
		/// <param name="fromBucketRevision">Min bucket revision</param>
		/// <param name="toBucketRevision">Max bucket revision</param>
		/// <param name="token">The Cancellation Token</param>
		/// <returns>List of streams identifiers</returns>
		Task<IEnumerable<Guid>> GetStreamIdsAsync(long fromBucketRevision = 1, long? toBucketRevision = null, CancellationToken token = default);
	}
}