using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NEStore
{
	public interface IBucket
	{
		string BucketName { get; }

		Task<WriteResult> WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<object> events);

		Task DispatchUndispatchedAsync();

		Task RollbackAsync(long bucketRevision);

		Task<IEnumerable<object>> GetEventsAsync(Guid? streamId = null, long? fromBucketRevision = null, long? toBucketRevision = null);

		Task<bool> HasUndispatchedCommitsAsync();

		Task<IEnumerable<CommitData>> GetCommitsAsync(Guid? streamId = null, long? fromBucketRevision = null, long? toBucketRevision = null);

		Task<long> GetBucketRevisionAsync();

		Task<int> GetStreamRevisionAsync(Guid streamId, long? atBucketRevision = null);

		Task<IEnumerable<Guid>> GetStreamIdsAsync(long? fromBucketRevision = null, long? toBucketRevision = null);
	}
}