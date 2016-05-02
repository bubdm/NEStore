using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace StreamLedger
{
	public interface IBucket
	{
		string BucketName { get; }

		Task<long> ReadBucketRevisionAsync();

		Task RollbackAsync(long bucketRevision);

		Task WriteStreamEventsAsync(Guid streamId, int fromStreamRevision, params object[] events);

		Task<IEnumerable<object>> ReadStreamEventsAsync(Guid streamId);
		Task<IEnumerable<object>> ReadStreamEventsAsync(Guid streamId, long fromBucketRevision, long toBucketRevision);
		Task<int> ReadStreamRevisionAsync(Guid streamId, long toBucketRevision);

		Task<IEnumerable<object>> ReadEventsAsync(long fromBucketRevision, long toBucketRevision);

		Task<IEnumerable<Guid>> ReadStreamIdsAsync();
		Task<IEnumerable<Guid>> ReadStreamIdsAsync(long fromBucketRevision, long toBucketRevision);
	}
}