using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace StreamLedger
{
	public interface IBucket
	{
		string BucketName { get; }

		Task WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<object> events);

		Task RollbackAsync(long bucketRevision);

		Task<IEnumerable<object>> EventsAsync(Guid streamId);
		Task<IEnumerable<object>> EventsAsync(Guid streamId, long fromBucketRevision, long toBucketRevision);

		Task<IEnumerable<CommitData>> CommitsAsync(long fromBucketRevision, long toBucketRevision);

		Task<long> BucketRevisionAsync();

		Task<int> StreamRevisionAsync(Guid streamId);
		Task<int> StreamRevisionAsync(Guid streamId, long atBucketRevision);

		Task<IEnumerable<Guid>> StreamIdsAsync();
		Task<IEnumerable<Guid>> StreamIdsAsync(long fromBucketRevision, long toBucketRevision);
	}
}