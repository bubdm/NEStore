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

		/// <summary>
		/// Persist a commit to Mongo
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="expectedStreamRevision">Expected revision of the provided stream</param>
		/// <param name="events">List of events to commit</param>
		/// <returns>WriteResult object containing the commit persisted and the DispatchTask of the events</returns>
		public async Task<WriteResult<T>> WriteAsync(Guid streamId, int expectedStreamRevision, IEnumerable<T> events)
		{
			if (expectedStreamRevision < 0)
				throw new ArgumentOutOfRangeException(nameof(expectedStreamRevision));

			await AutoEnsureIndexesAsync()
				.ConfigureAwait(false);

			var lastCommit = await GetLastCommitAsync()
				.ConfigureAwait(false);

			await CheckStreamConsistencyBeforeWriting(streamId, expectedStreamRevision, lastCommit)
				.ConfigureAwait(false);

			await _eventStore.UndispatchedStrategy
				.CheckUndispatchedAsync(this, streamId)
				.ConfigureAwait(false);

			var eventsArray = events.ToArray();
			var commit = await CreateCommitAsync(streamId, expectedStreamRevision, eventsArray, lastCommit).ConfigureAwait(false);

			try
			{
				await Collection.InsertOneAsync(commit)
					.ConfigureAwait(false);
			}
			catch (MongoWriteException ex)
			{
				if (ex.IsDuplicateKeyException())
					throw new ConcurrencyWriteException($"Someone else is working on the same bucket ({BucketName}) or stream ({commit.StreamId})", ex);
			}

			var dispatchTask = DispatchCommitAsync(commit);

			return new WriteResult<T>(commit, dispatchTask);
		}

		/// <summary>
		/// Dispatch all commits where dispatched attribute is set to false
		/// </summary>
		public async Task<CommitData<T>[]> DispatchUndispatchedAsync(Guid? streamId = null, long? toBucketRevision = null)
		{
			var filter = Builders<CommitData<T>>.Filter.Eq(p => p.Dispatched, false);
			if (streamId != null)
				filter = filter & Builders<CommitData<T>>.Filter.Eq(p => p.StreamId, streamId.Value);
			if (toBucketRevision != null)
				filter = filter & Builders<CommitData<T>>.Filter.Lte(p => p.BucketRevision, toBucketRevision.Value);

			var commits = await Collection
				.Find(filter)
				.Sort(Builders<CommitData<T>>.Sort.Ascending(p => p.BucketRevision))
				.ToListAsync()
				.ConfigureAwait(false);

			foreach (var commit in commits)
				await DispatchCommitAsync(commit)
					.ConfigureAwait(false);

			return commits.ToArray();
		}

		/// <summary>
		/// Set all undispatched events as dispatched, without dispatching them
		/// </summary>
		public async Task SetAllAsDispatched()
		{
		  await Collection
		    .UpdateManyAsync(Builders<CommitData<T>>.Filter.Eq(p => p.Dispatched, false),
		                     Builders<CommitData<T>>.Update.Set(p => p.Dispatched, true))
		    .ConfigureAwait(false);
		}

		/// <summary>
		/// Delete all commits succeeding the revision provided
		/// </summary>
		/// <param name="bucketRevision">Revision of last commit to keep</param>
		public async Task RollbackAsync(long bucketRevision)
		{
			await Collection
				.DeleteManyAsync(p => p.BucketRevision > bucketRevision).ConfigureAwait(false);

			await _eventStore.AutonIncrementStrategy.RollbackAsync(BucketName, bucketRevision).ConfigureAwait(false);
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

			var commitEvents = await Collection
				.Find(filter)
				.Sort(Builders<CommitData<T>>.Sort.Ascending(p => p.BucketRevision))
			  .Project(p => p.Events)
				.ToListAsync()
				.ConfigureAwait(false);
			
			var events = commitEvents.SelectMany(p => p).ToArray();

			var safeFrom = fromStreamRevision;
		  var safeTo = toStreamRevision ?? events.Length;

			return events
				.Skip(safeFrom - 1)
				.Take(safeTo - safeFrom + 1)
				.ToList();
		}

		/// <summary>
		/// Retrieve all commits from bucket filtered by params
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="fromBucketRevision">Start bucket revision</param>
		/// <param name="toBucketRevision">End bucket revision</param>
		/// <param name="dispatched">Include/exclude dispatched</param>
		/// <param name="limit">Limit</param>
		/// <returns>List of commits matching filters</returns>
		public async Task<IEnumerable<CommitData<T>>> GetCommitsAsync(
			Guid? streamId = null,
			long fromBucketRevision = 1,
			long? toBucketRevision = null,
			bool? dispatched = null,
			int? limit = null)
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

			if (dispatched != null)
				filter = filter & Builders<CommitData<T>>.Filter.Eq(p => p.Dispatched, dispatched.Value);

			var commits = await Collection
				.Find(filter)
				.Sort(Builders<CommitData<T>>.Sort.Ascending(p => p.BucketRevision))
				.Limit(limit)
				.ToListAsync()
				.ConfigureAwait(false);

			return commits;
		}

		/// <summary>
		/// Retrieve the latest commit matching the specified criteria
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="atBucketRevision">Get the last commit less or equal the specified bucket revision</param>
		/// <returns>Last commit info</returns>
		public async Task<CommitInfo> GetLastCommitAsync(Guid? streamId = null, long? atBucketRevision = null)
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
		/// <param name="expectedStreamRevision">Expected revision of the provided stream</param>
		/// <param name="eventsArray">List of events to commit</param>
		/// <param name="lastCommit">Last commit written to current bucket</param>
		/// <returns>CommitData object</returns>
		private async Task<CommitData<T>> CreateCommitAsync(Guid streamId, int expectedStreamRevision, T[] eventsArray, CommitInfo lastCommit)
		{
			var bucketRevision =await _eventStore.AutonIncrementStrategy
																	.IncrementAsync(BucketName, lastCommit)
																	.ConfigureAwait(false);

			var commit = new CommitData<T>
			{
				BucketRevision = bucketRevision,
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
			var dispatchers = _eventStore.GetDispatchers();

			await Task.WhenAll(dispatchers.Select(x => x.DispatchAsync(BucketName, commit)))
				.ConfigureAwait(false);

		  var commitBucketRevision = commit.BucketRevision;
		  await Collection.UpdateOneAsync(
		      p => p.BucketRevision == commitBucketRevision,
		      Builders<CommitData<T>>.Update.Set(p => p.Dispatched, true))
		    .ConfigureAwait(false);
		}

		/// <summary>
		/// Checks if someone else is writing on the same bucket
		/// </summary>
		/// <param name="streamId">Unique stream identifier</param>
		/// <param name="expectedStreamRevision">Expected revision of the provided stream</param>
		/// <param name="lastCommit">Last commit of the bucket</param>
		private async Task CheckStreamConsistencyBeforeWriting(Guid streamId, int expectedStreamRevision, CommitInfo lastCommit)
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