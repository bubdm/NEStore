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
		/// <param name="expectedStreamRevision">Expected revion of the provided stream</param>
		/// <param name="events">List of events to commit</param>
		/// <returns>WriteResult object containing the commit persisted and the DispatchTask of the events</returns>
		Task<WriteResult<T>> WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<T> events);

		/// <summary>
		/// Dispatch all commits where dispatched attribute is set to false
		/// </summary>
		/// <returns></returns>
		Task DispatchUndispatchedAsync();

		/// <summary>
		/// Set all undispatched events as dispatched, without dispatching them
		/// </summary>
		Task SetAllAsDispatched();

		/// <summary>
		/// Delete all commits succeeding the revision provided
		/// </summary>
		/// <param name="bucketRevision">Revision of last commit to keep</param>
		/// <returns></returns>
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
		/// Check if collection has undispatched commits
		/// </summary>
		/// <returns>True if at least one commit is undispatched, otherwise false</returns>
		Task<bool> HasUndispatchedCommitsAsync();

		/// <summary>
		/// Retrieve all commits from bucket filtered by params
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="fromBucketRevision">Start bucket revision</param>
		/// <param name="toBucketRevision">End bucket revision</param>
		/// <returns>List of commits matching filters</returns>
		Task<IEnumerable<CommitData<T>>> GetCommitsAsync(Guid? streamId = null, long fromBucketRevision = 1, long? toBucketRevision = null);

		/// <summary>
		/// Retrieve the current bucket revision
		/// </summary>
		/// <returns>Last commit revision, otherwise 0</returns>
		Task<long> GetBucketRevisionAsync();

		/// <summary>
		/// Retrieve the current stream revision
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="atBucketRevision">Max stream revision</param>
		/// <returns>Last stream revision, otherwise 0</returns>
		Task<int> GetStreamRevisionAsync(Guid streamId, long? atBucketRevision = null);

		/// <summary>
		/// Retrieve all streams inside the range provided
		/// </summary>
		/// <param name="fromBucketRevision">Min bucket revision</param>
		/// <param name="toBucketRevision">Max bucket revision</param>
		/// <returns>List of streams identifiers</returns>
		Task<IEnumerable<Guid>> GetStreamIdsAsync(long fromBucketRevision = 1, long? toBucketRevision = null);
	}
}