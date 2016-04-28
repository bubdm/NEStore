using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamLedger
{
	public interface IStreamLedger
	{
		Task EnsureBucketAsync(string bucketName);
		Task DeleteBucketAsync(string bucketName);

		Task<IBucketSession> OpenBucketAsync(string bucketName);
	}

	public interface IBucketSession
	{
		string BucketName { get; }
		long BucketRevision { get; }

		Task RollbackAsync(long bucketVersion);

		Task WriteAsync(Guid streamId, int streamRevision, params object[] events);

		Task<IEnumerable<object>> ReadAsync(long fromBucketRevision, long toBucketRevision);
		Task<IEnumerable<object>> ReadAsync(Guid streamId, long fromBucketRevision, long toBucketRevision);
	}
}
