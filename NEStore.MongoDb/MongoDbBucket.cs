using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace NEStore.MongoDb
{
	public class MongoDbBucket<T> : IBucket<T>
	{
		private bool _indexesEnsured;
		private readonly MongoDbEventStore<T> _eventStore;
		public IMongoCollection<CommitData<T>> Collection { get; }
		public string BucketName { get; }

		public MongoDbBucket(MongoDbEventStore<T> eventStore, string bucketName)
		{
			_eventStore = eventStore;
			Collection = eventStore.CollectionFromBucket(bucketName);
			BucketName = bucketName;
		}

		/// <summary>
		/// Persist a commit to Mongo
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="expectedStreamRevision">Expected revion of the provided stream</param>
		/// <param name="events">List of events to commit</param>
		/// <returns>WriteResult object containing the commit persisted and the DispatchTask of the events</returns>
		public async Task<WriteResult<T>> WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<T> events)
		{
			if (expectedStreamRevision < 0)
				throw new ArgumentOutOfRangeException(nameof(expectedStreamRevision));

			if (_eventStore.CheckStreamRevisionBeforeWriting)
			{
				// Note: this check doesn't ensure that in case of real concurrency no one can insert the same commit
				//  the real check is done via a mongo index "StreamRevision". This check basically just ensure to do not write 
				//  revision with holes
				var actualRevision = await GetStreamRevisionAsync(streamId)
																						.ConfigureAwait(false);
				if (actualRevision > expectedStreamRevision)
					throw new ConcurrencyWriteException("Someone else is working on the same bucket or stream");
				if (actualRevision < expectedStreamRevision) // Ensure to write commits sequentially
					throw new ArgumentOutOfRangeException(nameof(expectedStreamRevision));
			}

			await AutoEnsureIndexesAsync()
				.ConfigureAwait(false);

			await CheckForUndispatchedAsync()
				.ConfigureAwait(false);

			var eventsArray = events.ToArray();

			var commit = await CreateCommitAsync(streamId, expectedStreamRevision, eventsArray)
				.ConfigureAwait(false);

			try
			{
				await Collection.InsertOneAsync(commit)
					.ConfigureAwait(false);
			}
			catch (MongoWriteException ex)
			{
				if (ex.WriteError != null && ex.WriteError.Code == 11000)
					throw new ConcurrencyWriteException("Someone else is working on the same bucket or stream", ex);
			}

			var dispatchTask = DispatchCommitAsync(commit);

			return new WriteResult<T>(commit, dispatchTask);
		}

		/// <summary>
		/// Dispatch all commits where dispatched attribute is set to false
		/// </summary>
		/// <returns></returns>
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

		/// <summary>
		/// Set all undispatched events as dispatched, without dispatching them
		/// </summary>
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

		/// <summary>
		/// Delete all commits succeeding the revision provided
		/// </summary>
		/// <param name="bucketRevision">Revision of last commit to keep</param>
		/// <returns></returns>
		public Task RollbackAsync(long bucketRevision)
		{
			return Collection
				.DeleteManyAsync(p => p.BucketRevision > bucketRevision);
		}

		/// <summary>
		/// Retrieve all events from bucket filtered by params
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="fromBucketRevision">Start bucket revision</param>
		/// <param name="toBucketRevision">End bucket revision</param>
		/// <returns>Flattered list of events retrieved from commits</returns>
		public async Task<IEnumerable<T>> GetEventsAsync(Guid? streamId = null, long fromBucketRevision = 1, long? toBucketRevision = null)
		{
			var commits = await GetCommitsAsync(streamId, fromBucketRevision, toBucketRevision)
				.ConfigureAwait(false);

			return commits.SelectMany(c => c.Events);
		}

		/// <summary>
		/// Retrieve all events from bucket filtered by params
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="fromStreamRevision">Start stream revision</param>
		/// <param name="toStreamRevision">End stream revision</param>
		/// <returns>Flattered list of events retrieved from commits</returns>
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

		/// <summary>
		/// Check if collection has undispatched commits
		/// </summary>
		/// <returns>True if at least one commit is undispatched, otherwise false</returns>
		public async Task<bool> HasUndispatchedCommitsAsync()
		{
			return await Collection
				.Find(p => p.Dispatched == false)
				.AnyAsync()
				.ConfigureAwait(false);
		}

		/// <summary>
		/// Retrieve all commits from bucket filtered by params
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="fromBucketRevision">Start bucket revision</param>
		/// <param name="toBucketRevision">End bucket revision</param>
		/// <returns>List of commits matching filters</returns>
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

		/// <summary>
		/// Retrieve the current bucket revision
		/// </summary>
		/// <returns>Last commit revision, otherwise 0</returns>
		public async Task<long> GetBucketRevisionAsync()
		{
			var result = await Collection
				.Find(Builders<CommitData<T>>.Filter.Empty)
				.Sort(Builders<CommitData<T>>.Sort.Descending(p => p.BucketRevision))
				.FirstOrDefaultAsync()
				.ConfigureAwait(false);

			return result?.BucketRevision ?? 0;
		}

		/// <summary>
		/// Retrieve the current stream revision
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="atBucketRevision">Max stream revision</param>
		/// <returns>Last stream revision, otherwise 0</returns>
		public async Task<int> GetStreamRevisionAsync(Guid streamId, long? atBucketRevision = null)
		{
			if (atBucketRevision <= 0)
				throw new ArgumentOutOfRangeException(nameof(atBucketRevision), "Parameter must be greater than 0.");

			var filter = Builders<CommitData<T>>.Filter.Eq(p => p.StreamId, streamId);
			if (atBucketRevision != null)
				filter = filter & Builders<CommitData<T>>.Filter.Lte(p => p.BucketRevision, atBucketRevision.Value);

			var result = await Collection
				.Find(filter)
				.Sort(Builders<CommitData<T>>.Sort.Descending(p => p.BucketRevision))
				.FirstOrDefaultAsync()
				.ConfigureAwait(false);

			return result?.StreamRevisionEnd ?? 0;
		}

		/// <summary>
		/// Retrieve all streams inside the range provided
		/// </summary>
		/// <param name="fromBucketRevision">Min bucket revision</param>
		/// <param name="toBucketRevision">Max bucket revision</param>
		/// <returns>List of streams identifiers</returns>
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

		/// <summary>
		/// Create commit object that will be persisted to Mongo
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="expectedStreamRevision">Expected revion of the provided stream</param>
		/// <param name="eventsArray">List of events to commit</param>
		/// <returns></returns>
		private async Task<CommitData<T>> CreateCommitAsync(Guid streamId, int expectedStreamRevision, T[] eventsArray)
		{
			var commit = new CommitData<T>
			{
				BucketRevision = await GetBucketRevisionAsync().ConfigureAwait(false) + 1,
				Dispatched = false,
				Events = eventsArray,
				StreamId = streamId,
				StreamRevisionStart = expectedStreamRevision,
				StreamRevisionEnd = expectedStreamRevision + eventsArray.Length
			};
			return commit;
		}

		/// <summary>
		/// Setup bucket creating Indexes
		/// </summary>
		private async Task AutoEnsureIndexesAsync()
		{
			if (_indexesEnsured || !_eventStore.AutoEnsureIndexes)
				return;

			await _eventStore.EnsureBucketAsync(BucketName)
				.ConfigureAwait(false);
			_indexesEnsured = true;
		}

		/// <summary>
		/// Dispatch events of commit
		/// </summary>
		/// <param name="commit">Commit to be dispatched</param>
		private async Task DispatchCommitAsync(CommitData<T> commit)
		{
			await Task.WhenAll(_eventStore.GetDispatchers().Select(x => x.DispatchAsync(BucketName, commit)))
				.ConfigureAwait(false);

			await SetCommitsAsDispatched(commit)
				.ConfigureAwait(false);
		}

		/// <summary>
		/// Set commits as dispatched
		/// </summary>
		/// <param name="commits">List of commits dispatched</param>
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

		/// <summary>
		/// Check if bucket has undispatched commmits
		/// </summary>
		/// <returns>Throw UndispatchedEventsFoundException if undispatched commits exists</returns>
		private async Task CheckForUndispatchedAsync()
		{
			if (!_eventStore.AutoCheckUndispatched)
				return;

			var hasUndispatched = await HasUndispatchedCommitsAsync()
				.ConfigureAwait(false);
			if (hasUndispatched)
				throw new UndispatchedEventsFoundException("Undispatched events found, cannot write new events");

			// Eventually here I can try to dispatch undispatched to try to "recover" from a broken situations...
		}

	}
}