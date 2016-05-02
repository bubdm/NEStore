using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace StreamLedger.MongoDb
{
	public class MongoDbBucket : IBucket
	{
		public string BucketName { get; }
		public Task<long> ReadBucketRevisionAsync()
		{
			throw new NotImplementedException();
		}

		public Task RollbackAsync(long bucketRevision)
		{
			throw new NotImplementedException();
		}

		public Task WriteStreamEventsAsync(Guid streamId, int fromStreamRevision, params object[] events)
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<object>> ReadStreamEventsAsync(Guid streamId)
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<object>> ReadStreamEventsAsync(Guid streamId, long fromBucketRevision, long toBucketRevision)
		{
			throw new NotImplementedException();
		}

		public Task<int> ReadStreamRevisionAsync(Guid streamId, long toBucketRevision)
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<object>> ReadEventsAsync(long fromBucketRevision, long toBucketRevision)
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<Guid>> ReadStreamIdsAsync()
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<Guid>> ReadStreamIdsAsync(long fromBucketRevision, long toBucketRevision)
		{
			throw new NotImplementedException();
		}
	}
}