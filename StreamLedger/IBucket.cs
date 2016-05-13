using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace StreamLedger
{
	public interface IBucket
	{
		string BucketName { get; }

		Task WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<object> events);

		Task DispatchUndispatchedAsync();

		Task RollbackAsync(long bucketRevision);

		Task<IEnumerable<object>> GetEventsAsync(Guid streamId);
		Task<IEnumerable<object>> GetEventsAsync(Guid streamId, long fromBucketRevision, long toBucketRevision);

		Task<bool> HasUndispatchedCommitsAsync();

		Task<IEnumerable<CommitData>> GetCommitsAsync(long fromBucketRevision, long toBucketRevision);

		Task<long> GetBucketRevisionAsync();

		Task<int> GetStreamRevisionAsync(Guid streamId);
		Task<int> GetStreamRevisionAsync(Guid streamId, long atBucketRevision);

		Task<IEnumerable<Guid>> GetStreamIdsAsync();
		Task<IEnumerable<Guid>> GetStreamIdsAsync(long fromBucketRevision, long toBucketRevision);
	}
}