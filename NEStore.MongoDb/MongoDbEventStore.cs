using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace NEStore.MongoDb
{
	public class MongoDbEventStore<T> : IEventStore<T>
	{
		private readonly ConcurrentDictionary<string, MongoDbBucket<T>> _buckets = new ConcurrentDictionary<string, MongoDbBucket<T>>();
		private IDispatcher<T>[] _dispatchers = new IDispatcher<T>[0];
		public IMongoDatabase Database { get; }

		/// <summary>
		/// Configure the MongoDb write concern. Default is Acknowledged.
		/// </summary>
		public WriteConcern WriteConcern { get; set; } = WriteConcern.Acknowledged;
		/// <summary>
		/// Create indexes at first write. Default is true.
		/// </summary>
		public bool AutoEnsureIndexes { get; set; } = true;
		/// <summary>
		/// Check for undispatched events at each write. Default is true.
		/// </summary>
		public bool AutoCheckUndispatched { get; set; } = true;
		/// <summary>
		/// Manually check for stream revision validity before writing any data. Default is true.
		/// </summary>
		public bool CheckStreamRevisionBeforeWriting { get; set; } = true;

		static MongoDbEventStore()
		{
			BsonClassMap.RegisterClassMap<CommitData<T>>(cm =>
			{
				cm.MapIdProperty(c => c.BucketRevision);
				cm.AutoMap();
				cm.SetIgnoreExtraElements(true);
			});
		}

		public MongoDbEventStore(string connectionString)
		{
			var url = new MongoUrlBuilder(connectionString);

			if (string.IsNullOrWhiteSpace(url.DatabaseName))
				throw new ArgumentException("MongoDb connection string doesn't contain database");

			var client = new MongoClient(url.ToMongoUrl());

			Database = client.GetDatabase(url.DatabaseName);
		}

		/// <summary>
		/// Setup bucket creating Indexes
		/// </summary>
		/// <param name="bucketName">Bucket identifier</param>
		public async Task EnsureBucketAsync(string bucketName)
		{
			var collection = CollectionFromBucket(bucketName);

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

		public IMongoCollection<CommitData<T>> CollectionFromBucket(string bucketName)
		{
			return Database.GetCollection<CommitData<T>>(CollectionNameFromBucket(bucketName))
				.WithWriteConcern(WriteConcern);
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
	}
}
