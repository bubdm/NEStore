using System;
using System.Collections.Generic;
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
		/// <returns>WriteResult object containing the commit persisted and the DispatchTask of the events</returns>
		Task<WriteResult<T>> WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<T> events);

		/// <summary>
		/// Dispatch all commits where dispatched attribute is set to false
		/// </summary>
		Task DispatchUndispatchedAsync();

		/// <summary>
		/// Set all undispatched events as dispatched, without dispatching them
		/// </summary>
		Task SetAllAsDispatched();

		/// <summary>
		/// Delete all commits succeeding the revision provided
		/// </summary>
		/// <param name="bucketRevision">Revision of last commit to keep</param>
		Task RollbackAsync(long bucketRevision);

		/// <summary>
		/// Retrieve all events from bucket filtered by params
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="fromBucketRevision">Start bucket revision</param>
		/// <param name="toBucketRevision">End bucket revision</param>
		/// <returns>Flattered list of events retrieved from commits</returns>
		Task<IEnumerable<T>> GetEventsAsync(Guid? streamId = null, long fromBucketRevision = 1, long? toBucketRevision = null);

		/// <summary>
		/// Retrieve all events from bucket filtered by params
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="fromStreamRevision">Start stream revision</param>
		/// <param name="toStreamRevision">End stream revision</param>
		/// <returns>Flattered list of events retrieved from commits</returns>
		Task<IEnumerable<T>> GetEventsForStreamAsync(Guid streamId, int fromStreamRevision = 1, int? toStreamRevision = null);

		/// <summary>
		/// Retrieve all commits from bucket filtered by params
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="fromBucketRevision">Start bucket revision</param>
		/// <param name="toBucketRevision">End bucket revision</param>
		/// <returns>List of commits matching filters</returns>
		Task<IEnumerable<CommitData<T>>> GetCommitsAsync(Guid? streamId = null, long fromBucketRevision = 1, long? toBucketRevision = null);

		/// <summary>
		/// Retrieve the latest commit matching the specified criteria
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="atBucketRevision">Get the last commit less or equal the specified bucket revision</param>
		/// <returns>Last commit info</returns>
		Task<CommitInfo> GetLastCommitAsync(Guid? streamId = null, long? atBucketRevision = null);

		/// <summary>
		/// Retrieve all streams inside the range provided
		/// </summary>
		/// <param name="fromBucketRevision">Min bucket revision</param>
		/// <param name="toBucketRevision">Max bucket revision</param>
		/// <returns>List of streams identifiers</returns>
		Task<IEnumerable<Guid>> GetStreamIdsAsync(long fromBucketRevision = 1, long? toBucketRevision = null);
	}
}