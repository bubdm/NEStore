using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace StreamLedger.MongoDb
{
	public class MongoDbBucket : IBucket
	{
		public IMongoCollection<CommitData> Collection { get; }

		public MongoDbBucket(string bucketName, IMongoCollection<CommitData> collection)
		{
			Collection = collection;
			BucketName = bucketName;
		}

		public string BucketName { get; }

		public Task WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<object> events)
		{
			throw new NotImplementedException();
		}

		public Task RollbackAsync(long bucketRevision)
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<object>> EventsAsync(Guid streamId)
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<object>> EventsAsync(Guid streamId, long fromBucketRevision, long toBucketRevision)
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<CommitData>> CommitsAsync(long fromBucketRevision, long toBucketRevision)
		{
			throw new NotImplementedException();
		}

		public Task<long> BucketRevisionAsync()
		{
			throw new NotImplementedException();
		}

		public Task<int> StreamRevisionAsync(Guid streamId)
		{
			throw new NotImplementedException();
		}

		public Task<int> StreamRevisionAsync(Guid streamId, long atBucketRevision)
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<Guid>> StreamIdsAsync()
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<Guid>> StreamIdsAsync(long fromBucketRevision, long toBucketRevision)
		{
			throw new NotImplementedException();
		}
	}
}