using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace StreamLedger.MongoDb
{
	public class MongoDbBucket : IBucket
	{
		private readonly ILedger _ledger;
		public IMongoCollection<CommitData> Collection { get; }
		public string BucketName { get; }

		public MongoDbBucket(ILedger ledger, string bucketName, IMongoCollection<CommitData> collection)
		{
			_ledger = ledger;
			Collection = collection;
			BucketName = bucketName;
		}

		public Task WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<object> events)
		{
			throw new NotImplementedException();
		}

		public Task DispatchUndispatchedAsync()
		{
			throw new NotImplementedException();
		}

		public Task RollbackAsync(long bucketRevision)
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<object>> GetEventsAsync(Guid streamId)
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<object>> GetEventsAsync(Guid streamId, long fromBucketRevision, long toBucketRevision)
		{
			throw new NotImplementedException();
		}

		public Task<bool> HasUndispatchedCommitsAsync()
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<CommitData>> GetCommitsAsync(long fromBucketRevision, long toBucketRevision)
		{
			throw new NotImplementedException();
		}

		public async Task<long> GetBucketRevisionAsync()
		{
			var result = await Collection
				.Find(Builders<CommitData>.Filter.Empty)
				.Sort(Builders<CommitData>.Sort.Descending(p => p.BucketRevision))
				.FirstOrDefaultAsync();

			return result?.BucketRevision ?? 0;
		}

		public Task<int> GetStreamRevisionAsync(Guid streamId)
		{
			return GetStreamRevisionAsync(streamId, long.MaxValue);
		}

		public async Task<int> GetStreamRevisionAsync(Guid streamId, long atBucketRevision)
		{
			var filter = Builders<CommitData>.Filter.Eq(p => p.StreamId, streamId);
			if (atBucketRevision != long.MaxValue)
				filter = filter & Builders<CommitData>.Filter.Lte(p => p.BucketRevision, atBucketRevision);

			var result = await Collection
				.Find(filter)
				.Sort(Builders<CommitData>.Sort.Descending(p => p.BucketRevision))
				.FirstOrDefaultAsync();

			return result?.StreamRevisionEnd ?? 0;
		}

		public Task<IEnumerable<Guid>> GetStreamIdsAsync()
		{
			return GetStreamIdsAsync(long.MinValue, long.MaxValue);
		}

		public async Task<IEnumerable<Guid>> GetStreamIdsAsync(long fromBucketRevision, long toBucketRevision)
		{
			var filter = Builders<CommitData>.Filter.Empty;
			if (toBucketRevision != long.MaxValue)
				filter = filter & Builders<CommitData>.Filter.Lte(p => p.BucketRevision, toBucketRevision);
			if (fromBucketRevision != long.MinValue && fromBucketRevision != 0)
				filter = filter & Builders<CommitData>.Filter.Gte(p => p.BucketRevision, fromBucketRevision);

			var cursor = await Collection
				.DistinctAsync(p => p.StreamId, filter);
			var result = await cursor.ToListAsync();

			return result;
		}
	}
}