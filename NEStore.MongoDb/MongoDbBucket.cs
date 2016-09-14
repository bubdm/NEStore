using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace NEStore.MongoDb
{
	public class MongoDbBucket : IBucket
	{
		private bool _indexesEnsured;
		private readonly MongoDbEventStore _eventStore;
		public IMongoCollection<CommitData> Collection { get; }
		public string BucketName { get; }

		public bool CheckStreamRevisionBeforeWriting { get; set; } = true;

		public MongoDbBucket(MongoDbEventStore eventStore, string bucketName, IMongoCollection<CommitData> collection)
		{
			_eventStore = eventStore;
			Collection = collection;
			BucketName = bucketName;
		}

		public async Task<WriteResult> WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<object> events)
		{
			if (expectedStreamRevision < 0)
				throw new ArgumentOutOfRangeException(nameof(expectedStreamRevision));

			if (CheckStreamRevisionBeforeWriting)
			{
				// Note: this check doesn't ensure that in case of real concurrency no one can insert the same commit
				//  the real check is done via a mongo index "StreamRevision"
				var actualRevision = await GetStreamRevisionAsync(streamId);
				if (actualRevision > expectedStreamRevision)
					throw new ConcurrencyWriteException("Someone else is working on the same bucket or stream");
				if (actualRevision < expectedStreamRevision) // Ensure to write commits sequentially
					throw new ArgumentOutOfRangeException(nameof(expectedStreamRevision));
			}

			await AutoEnsureIndexesAsync();

			await CheckForUndispatchedAsync();

			var eventsArray = events.ToArray();

			var commit = await CreateCommitAsync(streamId, expectedStreamRevision, eventsArray);

			try
			{
				await Collection.InsertOneAsync(commit);
			}
			catch (MongoWriteException ex)
			{
				if (ex.WriteError != null && ex.WriteError.Code == 11000)
					throw new ConcurrencyWriteException("Someone else is working on the same bucket or stream", ex);
			}

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
			return Collection
				.DeleteManyAsync(p => p.BucketRevision > bucketRevision);
		}

		public async Task<IEnumerable<object>> GetEventsAsync(Guid? streamId = null, long? fromBucketRevision = null, long? toBucketRevision = null)
		{
			var commits = await GetCommitsAsync(streamId, fromBucketRevision, toBucketRevision);

			return commits.SelectMany(c => c.Events);
		}

		public async Task<bool> HasUndispatchedCommitsAsync()
		{
			return await Collection
				.Find(p => p.Dispatched == false)
				.AnyAsync();
		}

		public async Task<IEnumerable<CommitData>> GetCommitsAsync(Guid? streamId = null, long? fromBucketRevision = null, long? toBucketRevision = null)
		{
			var filter = Builders<CommitData>.Filter.Empty;
			if (streamId != null)
				filter = filter & Builders<CommitData>.Filter.Eq(p => p.StreamId, streamId.Value);
			if (fromBucketRevision != null && fromBucketRevision != long.MinValue)
				filter = filter & Builders<CommitData>.Filter.Gte(p => p.BucketRevision, fromBucketRevision.Value);
			if (toBucketRevision != null && toBucketRevision != long.MaxValue)
				filter = filter & Builders<CommitData>.Filter.Lte(p => p.BucketRevision, toBucketRevision.Value);

			var commits = await Collection
				.Find(filter)
				.Sort(Builders<CommitData>.Sort.Ascending(p => p.BucketRevision))
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

		public async Task<int> GetStreamRevisionAsync(Guid streamId, long? atBucketRevision = null)
		{
			var filter = Builders<CommitData>.Filter.Eq(p => p.StreamId, streamId);
			if (atBucketRevision != null && atBucketRevision != long.MaxValue)
				filter = filter & Builders<CommitData>.Filter.Lte(p => p.BucketRevision, atBucketRevision.Value);

			var result = await Collection
				.Find(filter)
				.Sort(Builders<CommitData>.Sort.Descending(p => p.BucketRevision))
				.FirstOrDefaultAsync();

			return result?.StreamRevisionEnd ?? 0;
		}

		public async Task<IEnumerable<Guid>> GetStreamIdsAsync(long? fromBucketRevision = null, long? toBucketRevision = null)
		{
			var filter = Builders<CommitData>.Filter.Empty;
			if (fromBucketRevision != null && fromBucketRevision != long.MinValue)
				filter = filter & Builders<CommitData>.Filter.Gte(p => p.BucketRevision, fromBucketRevision.Value);
			if (toBucketRevision != null && toBucketRevision != long.MaxValue)
				filter = filter & Builders<CommitData>.Filter.Lte(p => p.BucketRevision, toBucketRevision.Value);

			var cursor = await Collection
				.DistinctAsync(p => p.StreamId, filter);
			var result = await cursor.ToListAsync();

			return result;
		}


		private async Task<CommitData> CreateCommitAsync(Guid streamId, int expectedStreamRevision, object[] eventsArray)
		{
			var commit = new CommitData
			{
				BucketRevision = await GetBucketRevisionAsync() + 1,
				Dispatched = false,
				Events = eventsArray,
				StreamId = streamId,
				StreamRevisionStart = expectedStreamRevision,
				StreamRevisionEnd = expectedStreamRevision + eventsArray.Length
			};
			return commit;
		}

		private async Task AutoEnsureIndexesAsync()
		{
			if (_indexesEnsured || !_eventStore.AutoEnsureIndexes)
				return;

			await _eventStore.EnsureBucketAsync(BucketName);
			_indexesEnsured = true;
		}

	    private async Task DispatchCommitAsync(CommitData commit)
	    {
	        // TODO Eval if dispath on dispatcher in parallel
	        foreach (var dispatcher in _eventStore.GetDispatchers())
	        {
	            foreach (var e in commit.Events)
	                await dispatcher.DispatchAsync(e);
	        }

	        await Collection.UpdateOneAsync(
	            p => p.BucketRevision == commit.BucketRevision,
	            Builders<CommitData>.Update.Set(p => p.Dispatched, true));
	    }


	    private async Task CheckForUndispatchedAsync()
		{
			if (!_eventStore.AutoCheckUndispatched)
				return;

			var found = await HasUndispatchedCommitsAsync();
			if (found)
				throw new UndispatchedEventsFoundException("Undispatched events found, cannot write new events");
		}

	}
}