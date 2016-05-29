using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace StreamLedger.MongoDb
{
	public class MongoDbBucket : IBucket
	{
		private bool _indexesEnsured;
		private readonly MongoDbLedger _ledger;
		public IMongoCollection<CommitData> Collection { get; }
		public string BucketName { get; }

		public MongoDbBucket(MongoDbLedger ledger, string bucketName, IMongoCollection<CommitData> collection)
		{
			_ledger = ledger;
			Collection = collection;
			BucketName = bucketName;
		}

		public async Task<WriteResult> WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<object> events)
		{
			await AutoEnsureIndexesAsync();

			var eventsArray = events.ToArray();

			var commit = new CommitData
			{
				BucketRevision = await GetBucketRevisionAsync() + 1,
				Dispatched = false,
				Events = eventsArray,
				StreamId = streamId,
				StreamRevisionStart = expectedStreamRevision,
				StreamRevisionEnd = expectedStreamRevision + eventsArray.Length
			};

			await Collection.InsertOneAsync(commit);

			var dispatchTask = DispatchCommitAsync(commit);

			return new WriteResult(commit, dispatchTask);
		}

		public async Task DispatchUndispatchedAsync()
		{
			var commits = await Collection
				.Find(p => p.Dispatched == false)
				.Sort(Builders<CommitData>.Sort.Ascending(p => p.BucketRevision))
				.ToListAsync();

			foreach (var commit in commits)
				await DispatchCommitAsync(commit);
		}

		public Task RollbackAsync(long bucketRevision)
		{
			throw new NotImplementedException();
		}

		public Task<IEnumerable<object>> GetEventsAsync(Guid streamId)
		{
			return GetEventsAsync(streamId, long.MinValue, long.MaxValue);
		}

		public async Task<IEnumerable<object>> GetEventsAsync(Guid streamId, long fromBucketRevision, long toBucketRevision)
		{
			var filter = Builders<CommitData>.Filter.Eq(p => p.StreamId, streamId);
			if (fromBucketRevision != long.MinValue || fromBucketRevision != 0)
				filter = filter & Builders<CommitData>.Filter.Gte(p => p.BucketRevision, fromBucketRevision);
			if (toBucketRevision != long.MaxValue)
				filter = filter & Builders<CommitData>.Filter.Lte(p => p.BucketRevision, toBucketRevision);

			var commits = await Collection
				.Find(filter)
				.Sort(Builders<CommitData>.Sort.Ascending(p => p.BucketRevision))
				.ToListAsync();

			return commits.SelectMany(c => c.Events);
		}

		public async Task<bool> HasUndispatchedCommitsAsync()
		{
			return await Collection
				.Find(p => p.Dispatched == false)
				.AnyAsync();
		}

		public async Task<IEnumerable<CommitData>> GetCommitsAsync(long fromBucketRevision, long toBucketRevision)
		{
			var filter = Builders<CommitData>.Filter.Empty;
			if (fromBucketRevision != long.MinValue || fromBucketRevision != 0)
				filter = filter & Builders<CommitData>.Filter.Gte(p => p.BucketRevision, fromBucketRevision);
			if (toBucketRevision != long.MaxValue)
				filter = filter & Builders<CommitData>.Filter.Lte(p => p.BucketRevision, toBucketRevision);

			var commits = await Collection
				.Find(filter)
				.Sort(Builders<CommitData>.Sort.Descending(p => p.BucketRevision))
				.ToListAsync();

			return commits;
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
			if (fromBucketRevision != long.MinValue && fromBucketRevision != 0)
				filter = filter & Builders<CommitData>.Filter.Gte(p => p.BucketRevision, fromBucketRevision);
			if (toBucketRevision != long.MaxValue)
				filter = filter & Builders<CommitData>.Filter.Lte(p => p.BucketRevision, toBucketRevision);

			var cursor = await Collection
				.DistinctAsync(p => p.StreamId, filter);
			var result = await cursor.ToListAsync();

			return result;
		}

		private async Task AutoEnsureIndexesAsync()
		{
			if (_indexesEnsured || !_ledger.AutoEnsureIndexes)
				return;

			await _ledger.EnsureBucketAsync(BucketName);
			_indexesEnsured = true;
		}

		private async Task DispatchCommitAsync(CommitData commit)
		{
			// TODO Eval if dispath on dispatcher in parallel
			foreach (var dispatcher in _ledger.GetDispatchers())
			{
				foreach (var e in commit.Events)
					await dispatcher.DispatchAsync(e);
			}

			await Collection.UpdateOneAsync(
				p => p.BucketRevision == commit.BucketRevision,
				Builders<CommitData>.Update.Set(p => p.Dispatched, true));
		}
	}
}