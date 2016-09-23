using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NEStore
{
	public interface IBucket<T>
	{
		string BucketName { get; }

		Task<WriteResult<T>> WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<T> events);

		Task DispatchUndispatchedAsync();

		Task SetAllAsDispatched();

		Task RollbackAsync(long bucketRevision);

		Task<IEnumerable<T>> GetEventsAsync(Guid? streamId = null, long fromBucketRevision = 1, long? toBucketRevision = null);

		Task<IEnumerable<T>> GetEventsForStreamAsync(Guid streamId, int fromStreamRevision = 1, int? toStreamRevision = null);

		Task<IEnumerable<CommitData<T>>> GetCommitsAsync(Guid? streamId = null, long fromBucketRevision = 1, long? toBucketRevision = null);

		Task<CommitInfo> GetLastCommit(Guid? streamId = null, long? atBucketRevision = null);

		Task<IEnumerable<Guid>> GetStreamIdsAsync(long fromBucketRevision = 1, long? toBucketRevision = null);
	}
}