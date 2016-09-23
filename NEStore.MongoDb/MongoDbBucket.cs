using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace NEStore.MongoDb
{
	public class MongoDbBucket<T> : IBucket<T>
	{
		private volatile bool _indexesEnsured;
		private readonly MongoDbEventStore<T> _eventStore;
		public IMongoCollection<CommitData<T>> Collection { get; }
		public IMongoCollection<CommitInfo> InfoCollection { get; }
		public string BucketName { get; }

		public MongoDbBucket(MongoDbEventStore<T> eventStore, string bucketName)
		{
			_eventStore = eventStore;
			Collection = eventStore.CollectionFromBucket<CommitData<T>>(bucketName);
			InfoCollection = eventStore.CollectionFromBucket<CommitInfo>(bucketName);
			BucketName = bucketName;
		}

		public async Task<WriteResult<T>> WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<T> events)
		{
			if (expectedStreamRevision < 0)
				throw new ArgumentOutOfRangeException(nameof(expectedStreamRevision));

			var lastCommit = await GetLastCommit()
				.ConfigureAwait(false);

			await CheckBeforeWriting(streamId, expectedStreamRevision, lastCommit)
				.ConfigureAwait(false);

			await AutoEnsureIndexesAsync()
				.ConfigureAwait(false);

			if (lastCommit != null)
				CheckForUndispatched(lastCommit.Dispatched);

			var eventsArray = events.ToArray();

			var commit = CreateCommitAsync(streamId, expectedStreamRevision, eventsArray, lastCommit?.BucketRevision ?? 0);

			try
			{
				await Collection.InsertOneAsync(commit)
					.ConfigureAwait(false);
			}
			catch (MongoWriteException ex)
			{
				if (IsMongoExceptionDuplicateKey(ex))
					throw new ConcurrencyWriteException("Someone else is working on the same bucket or stream", ex);
			}

			var dispatchTask = DispatchCommitAsync(commit);

			return new WriteResult<T>(commit, dispatchTask);
		}

		public async Task DispatchUndispatchedAsync()
		{
			var commits = await Collection
				.Find(p => p.Dispatched == false)
				.Sort(Builders<CommitData<T>>.Sort.Ascending(p => p.BucketRevision))
				.ToListAsync()
				.ConfigureAwait(false);

			foreach (var commit in commits)
				await DispatchCommitAsync(commit)
					.ConfigureAwait(false);
		}

		public async Task SetAllAsDispatched()
		{
			var commits = await Collection
				.Find(p => p.Dispatched == false)
				.Sort(Builders<CommitData<T>>.Sort.Ascending(p => p.BucketRevision))
				.ToListAsync()
				.ConfigureAwait(false);

			await SetCommitsAsDispatched(commits.ToArray())
				.ConfigureAwait(false);
		}

		public Task RollbackAsync(long bucketRevision)
		{
			return Collection
				.DeleteManyAsync(p => p.BucketRevision > bucketRevision);
		}

		public async Task<IEnumerable<T>> GetEventsAsync(Guid? streamId = null, long fromBucketRevision = 1, long? toBucketRevision = null)
		{
			var commits = await GetCommitsAsync(streamId, fromBucketRevision, toBucketRevision)
				.ConfigureAwait(false);

			return commits.SelectMany(c => c.Events);
		}

		public async Task<IEnumerable<T>> GetEventsForStreamAsync(Guid streamId, int fromStreamRevision = 1, int? toStreamRevision = null)
		{
			if (fromStreamRevision <= 0)
				throw new ArgumentOutOfRangeException(nameof(fromStreamRevision),
						"Parameter must be greater than 0.");

			if (toStreamRevision <= 0)
				throw new ArgumentOutOfRangeException(nameof(toStreamRevision), "Parameter must be greater than 0.");

			var filter = Builders<CommitData<T>>.Filter.Eq(p => p.StreamId, streamId);

			// Note: I cannot filter the start because I don't want to query mongo using non indexed fields
			//  and I assume that for one stream we should not have so many events ...
			if (toStreamRevision != null)
				filter = filter & Builders<CommitData<T>>.Filter.Lt(p => p.StreamRevisionStart, toStreamRevision.Value);

			var commits = await Collection
				.Find(filter)
				.Sort(Builders<CommitData<T>>.Sort.Ascending(p => p.BucketRevision))
				.ToListAsync()
				.ConfigureAwait(false);

			var events = commits
				.SelectMany(c => c.Events)
				.ToList();

			var safeFrom = fromStreamRevision;
			var safeTo = toStreamRevision ?? events.Count;

			return events
				.Skip(safeFrom - 1)
				.Take(safeTo - safeFrom + 1)
				.ToList();
		}

		public async Task<bool> HasUndispatchedCommitsAsync()
		{
			return await Collection
				.Find(p => p.Dispatched == false)
				.AnyAsync()
				.ConfigureAwait(false);
		}

		public async Task<IEnumerable<CommitData<T>>> GetCommitsAsync(Guid? streamId = null, long fromBucketRevision = 1, long? toBucketRevision = null)
		{
			if (fromBucketRevision <= 0)
				throw new ArgumentOutOfRangeException(nameof(fromBucketRevision),
						"Parameter must be greater than 0.");

			if (toBucketRevision <= 0)
				throw new ArgumentOutOfRangeException(nameof(toBucketRevision), "Parameter must be greater than 0.");

			var filter = Builders<CommitData<T>>.Filter.Empty;
			if (streamId != null)
				filter = filter & Builders<CommitData<T>>.Filter.Eq(p => p.StreamId, streamId.Value);

			if (fromBucketRevision != 1)
				filter = filter & Builders<CommitData<T>>.Filter.Gte(p => p.BucketRevision, fromBucketRevision);
			if (toBucketRevision != null)
				filter = filter & Builders<CommitData<T>>.Filter.Lte(p => p.BucketRevision, toBucketRevision.Value);

			var commits = await Collection
				.Find(filter)
				.Sort(Builders<CommitData<T>>.Sort.Ascending(p => p.BucketRevision))
				.ToListAsync()
				.ConfigureAwait(false);

			return commits;
		}

		public async Task<CommitInfo> GetLastCommit(Guid? streamId = null, long? atBucketRevision = null)
		{
			if (atBucketRevision <= 0)
				throw new ArgumentOutOfRangeException(nameof(atBucketRevision), "Parameter must be greater than 0.");

			var filter = Builders<CommitInfo>.Filter.Empty;
			if (streamId != null)
				filter = filter & Builders<CommitInfo>.Filter.Eq(p => p.StreamId, streamId.Value);
			if (atBucketRevision != null)
				filter = filter & Builders<CommitInfo>.Filter.Lte(p => p.BucketRevision, atBucketRevision.Value);

			var result = await InfoCollection
				.Find(filter)
				.Sort(Builders<CommitInfo>.Sort.Descending(p => p.BucketRevision))
				.FirstOrDefaultAsync()
				.ConfigureAwait(false);

			return result;
		}

		public async Task<IEnumerable<Guid>> GetStreamIdsAsync(long fromBucketRevision = 1, long? toBucketRevision = null)
		{
			if (fromBucketRevision <= 0)
				throw new ArgumentOutOfRangeException(nameof(fromBucketRevision), "Parameter must be greater than 0.");

			if (toBucketRevision <= 0)
				throw new ArgumentOutOfRangeException(nameof(toBucketRevision), "Parameter must be greater than 0.");

			var filter = Builders<CommitData<T>>.Filter.Empty;
			if (fromBucketRevision != 1)
				filter = filter & Builders<CommitData<T>>.Filter.Gte(p => p.BucketRevision, fromBucketRevision);
			if (toBucketRevision != null)
				filter = filter & Builders<CommitData<T>>.Filter.Lte(p => p.BucketRevision, toBucketRevision.Value);

			var cursor = await Collection
				.DistinctAsync(p => p.StreamId, filter)
				.ConfigureAwait(false);
			var result = await cursor.ToListAsync()
				.ConfigureAwait(false);

			return result;
		}


		private static CommitData<T> CreateCommitAsync(Guid streamId, int expectedStreamRevision, T[] eventsArray, long bucketRevision)
		{
			var commit = new CommitData<T>
			{
				BucketRevision = bucketRevision + 1,
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

			await _eventStore.EnsureBucketAsync(BucketName)
				.ConfigureAwait(false);
			_indexesEnsured = true;
		}

		private async Task DispatchCommitAsync(CommitData<T> commit)
		{
			await Task.WhenAll(_eventStore.GetDispatchers().Select(x => x.DispatchAsync(BucketName, commit)))
				.ConfigureAwait(false);

			await SetCommitsAsDispatched(commit)
				.ConfigureAwait(false);
		}

		private async Task SetCommitsAsDispatched(params CommitData<T>[] commits)
		{
			foreach (var commit in commits)
			{
				await Collection.UpdateOneAsync(
					p => p.BucketRevision == commit.BucketRevision,
					Builders<CommitData<T>>.Update.Set(p => p.Dispatched, true))
					.ConfigureAwait(false);
			}
		}

		private void CheckForUndispatched(bool lastCommitdDispatched)
		{
			if (!_eventStore.AutoCheckUndispatched)
				return;

			var hasUndispatched = !lastCommitdDispatched;
			if (hasUndispatched)
				throw new UndispatchedEventsFoundException("Undispatched events found, cannot write new events");

			// Eventually here I can try to dispatch undispatched to try to "recover" from a broken situations...
		}

		private static bool IsMongoExceptionDuplicateKey(MongoWriteException ex)
		{
			return ex.WriteError != null && ex.WriteError.Code == 11000;
		}

		private async Task CheckBeforeWriting(Guid streamId, int expectedStreamRevision, CommitInfo lastCommit)
		{
			if (!_eventStore.CheckStreamRevisionBeforeWriting)
				return;

			if (lastCommit == null)
				return;

			var lastStreamRevision = lastCommit.StreamId == streamId
				? lastCommit.StreamRevisionEnd
				: await this.GetStreamRevisionAsync(streamId)
					.ConfigureAwait(false);

			// Note: this check doesn't ensure that in case of real concurrency no one can insert the same commit
			//  the real check is done via a mongo index "StreamRevision". This check basically just ensure to do not write 
			//  revision with holes
			if (lastStreamRevision > expectedStreamRevision)
				throw new ConcurrencyWriteException("Someone else is working on the same bucket or stream");
			if (lastStreamRevision < expectedStreamRevision) // Ensure to write commits sequentially
				throw new ArgumentOutOfRangeException(nameof(expectedStreamRevision));
		}
	}
}