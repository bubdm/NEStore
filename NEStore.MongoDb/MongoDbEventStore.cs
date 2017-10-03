using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using NEStore.MongoDb.AutoIncrementStrategies;

namespace NEStore.MongoDb
{
	public class MongoDbEventStore<T> : IEventStore<T>
	{
		private readonly ConcurrentDictionary<string, MongoDbBucket<T>> _buckets = new ConcurrentDictionary<string, MongoDbBucket<T>>();
		private IDispatcher<T>[] _dispatchers = new IDispatcher<T>[0];
		public IMongoDatabase Database { get; }

		/// <summary>
		/// Create indexes at first write. Default is true.
		/// </summary>
		public bool AutoEnsureIndexes { get; set; } = true;
		/// <summary>
		/// Check for undispatched events at each write. Default is true.
		/// </summary>
		public bool AutoCheckUndispatched { get; set; } = true;
		/// <summary>
		/// Dispatch undispatched events at each write. Default is true.
		/// </summary>
		public bool AutoDispatchUndispatchedOnWrite { get; set; } = true;
		/// <summary>
		/// When undispatched events are found, wait and double-check for undispatched status. Default is 1s.
		/// NOTE: Events are supposed to be idempotent, so they can be eventualy redispatched multiple times,
		///  but I want to ensure that this not happen in a short period of time.
		/// The same event can be redispatched in case of a temporary network problem, db problem, ...
		///  but normally the system ensure that an event is dispatched only once, also on heavy load scenario (concurrency)
		/// The idea is to ensure this by waiting (AutoDispatchWaitTime) and check if the system is busy doing dispatching.
		/// If the system doesn't dispatch any new events after the AutoDispatchWaitTime then it is "safe" to try to redispatch it
		/// </summary>
		public TimeSpan AutoDispatchWaitTime { get; set; } = TimeSpan.FromSeconds(1);
		/// <summary>
		/// Manually check for stream revision validity before writing any data. Default is true.
		/// </summary>
		public bool CheckStreamRevisionBeforeWriting { get; set; } = true;
		/// <summary>
		/// 
		/// </summary>
		public IAutoIncrementStrategy AutonIncrementStrategy { get; set; }

		static MongoDbEventStore()
		{
			MongoDbSerialization.RegisterCommitData<T>();
		}

		public MongoDbEventStore(string connectionString)
			:this(connectionString, GetDefaultDatabaseSettings(connectionString))
		{
		}

		public MongoDbEventStore(string connectionString, MongoDatabaseSettings settings)
		{
			var url = new MongoUrlBuilder(connectionString);

			if (string.IsNullOrWhiteSpace(url.DatabaseName))
				throw new ArgumentException("MongoDb connection string doesn't contain database");
			
			var client = new MongoClient(url.ToMongoUrl());
			Database = client.GetDatabase(url.DatabaseName, settings);

			AutonIncrementStrategy = new IncrementCountersStrategy<T>(this);
		}

		/// <summary>
		/// Setup bucket creating Indexes
		/// </summary>
		/// <param name="bucketName">Bucket identifier</param>
		public async Task EnsureBucketAsync(string bucketName)
		{
			var collection = CollectionFromBucket<CommitData<T>>(bucketName);

			var builder = new IndexKeysDefinitionBuilder<CommitData<T>>();

			await collection.Indexes.CreateManyAsync(new[]
			{
				// BucketRevision is _id (automatically indexed and unique)
				new CreateIndexModel<CommitData<T>>(builder
					.Ascending(p => p.Dispatched), new CreateIndexOptions { Name = "Dispatched" }),
				new CreateIndexModel<CommitData<T>>(builder
					.Ascending(p => p.StreamId), new CreateIndexOptions { Name = "StreamId" }),
				new CreateIndexModel<CommitData<T>>(builder
					.Ascending(p => p.StreamId)
					.Ascending(p => p.StreamRevisionStart), new CreateIndexOptions { Name = "StreamRevision", Unique = true })
			}).ConfigureAwait(false);
		}

		/// <summary>
		/// Drop bucket from Mongo
		/// </summary>
		/// <param name="bucketName">Bucket identifier</param>
		public async Task DeleteBucketAsync(string bucketName)
		{
			await Database.DropCollectionAsync(CollectionNameFromBucket(bucketName))
										.ConfigureAwait(false);

			await AutonIncrementStrategy.DeleteBucketAsync(bucketName).ConfigureAwait(false);
		}

		/// <summary>
		/// Provide the bucket instance
		/// </summary>
		/// <param name="bucketName">Bucket identifier</param>
		public IBucket<T> Bucket(string bucketName)
		{
			return _buckets.GetOrAdd(
				bucketName,
				b => new MongoDbBucket<T>(this, b)
				);
		}

		/// <summary>
		/// Register dispatchers
		/// </summary>
		/// <param name="dispatchers">List of dispatchers</param>
		public void RegisterDispatchers(params IDispatcher<T>[] dispatchers)
		{
			_dispatchers = dispatchers;
		}

		/// <summary>
		/// Return registered dispatchers
		/// </summary>
		/// <returns>List of dispatchers</returns>
		public IEnumerable<IDispatcher<T>> GetDispatchers()
		{
			return _dispatchers;
		}

		/// <summary>
		/// Retrieves Collection for provided bucket
		/// </summary>
		/// <param name="bucketName">Bucket identifier</param>
		/// <returns>Mongo collection</returns>
		public IMongoCollection<TDoc> CollectionFromBucket<TDoc>(string bucketName)
		{
			return Database
				.GetCollection<TDoc>(CollectionNameFromBucket(bucketName));
		}

		/// <summary>
		/// Format collection for a bucket
		/// </summary>
		/// <param name="bucketName">Bucket identifier</param>
		/// <returns>Collection name</returns>
		private static string CollectionNameFromBucket(string bucketName)
		{
			if (!IsValidBucketName(bucketName))
				throw new ArgumentException("Invalid bucket name");

			return $"{bucketName}.commits";
		}

		/// <summary>
		/// Validate bucket name
		/// </summary>
		/// <param name="bucketName">Bucket identifier</param>
		/// <returns>True if name is valid, otherwise false</returns>
		private static bool IsValidBucketName(string bucketName)
		{
			if (string.IsNullOrWhiteSpace(bucketName))
				return false;

			var regExp = new Regex("^[a-z]+$");
			return regExp.IsMatch(bucketName);
		}

		/// <summary>
		/// Get the default mongodb settings used.
		/// Default values:
		/// 	GuidRepresentation = Standard,
		///		WriteConcern = 1 with journal,
		///		ReadConcern = local,
		///		ReadPreference = Primary
		/// </summary>
		/// <returns></returns>
		public static MongoDatabaseSettings GetDefaultDatabaseSettings(string connectionString)
		{
			var url = new MongoUrl(connectionString);

			// var supportsCommittedReads = IsCommittedReadsSupported(url);
			// in the past the default was: supportsCommittedReads ? ReadConcern.Majority : ReadConcern.Default;
			//  but this doesn't work if writeConcern is not majority
			var readConcern = ReadConcern.Default;
			if (url.ReadConcernLevel.HasValue)
				readConcern = new ReadConcern(url.ReadConcernLevel);

			var writeConcern = url.W != null
				? new WriteConcern(w: url.W, journal: url.Journal ?? true)
				: new WriteConcern(journal: url.Journal ?? true);

			var dbSettings = new MongoDatabaseSettings()
			{
				GuidRepresentation = connectionString.Contains("uuidRepresentation=") ? url.GuidRepresentation : GuidRepresentation.Standard,
				WriteConcern = writeConcern,
				ReadConcern = readConcern,
				ReadPreference = url.ReadPreference ?? ReadPreference.Primary
			};
			
			return dbSettings;
		}

		//private static bool IsCommittedReadsSupported(MongoUrl url)
		//{
		//	var client = new MongoClient(url);
		//	var status = client.GetDatabase("admin")
		//		.RunCommand<BsonDocument>(new BsonDocument("serverStatus", 1));

		//	var supportsCommittedReads = false;
		//	BsonElement storageEngineElement;
		//	if (status.TryGetElement("storageEngine", out storageEngineElement))
		//	{
		//		var storageEngineDoc = storageEngineElement.Value as BsonDocument;
		//		BsonValue supportsCommittedReadsValue;
		//		if (storageEngineDoc != null
		//		    && storageEngineDoc.TryGetValue("supportsCommittedReads", out supportsCommittedReadsValue))
		//		{
		//			supportsCommittedReads = supportsCommittedReadsValue.AsBoolean;
		//		}
		//	}
		//	return supportsCommittedReads;
		//}
	}
}
