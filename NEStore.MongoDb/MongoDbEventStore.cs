using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
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
			MongoDbSerialization.RegisterCommitData<T>();
		}

		public MongoDbEventStore(string connectionString)
		{
			var url = new MongoUrlBuilder(connectionString);

			if (string.IsNullOrWhiteSpace(url.DatabaseName))
				throw new ArgumentException("MongoDb connection string doesn't contain database");

			var client = new MongoClient(url.ToMongoUrl());

			Database = client.GetDatabase(url.DatabaseName);
		}

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

		public async Task DeleteBucketAsync(string bucketName)
		{
			await Database.DropCollectionAsync(CollectionNameFromBucket(bucketName))
										.ConfigureAwait(false);
		}

		public IBucket<T> Bucket(string bucketName)
		{
			return _buckets.GetOrAdd(
				bucketName,
				b => new MongoDbBucket<T>(this, b)
				);
		}

		public void RegisterDispatchers(params IDispatcher<T>[] dispatchers)
		{
			_dispatchers = dispatchers;
		}

		public IEnumerable<IDispatcher<T>> GetDispatchers()
		{
			return _dispatchers;
		}

		public IMongoCollection<TDoc> CollectionFromBucket<TDoc>(string bucketName)
		{
			return Database.GetCollection<TDoc>(CollectionNameFromBucket(bucketName))
				.WithWriteConcern(WriteConcern);
		}

		private static string CollectionNameFromBucket(string bucketName)
		{
			if (!IsValidBucketName(bucketName))
				throw new ArgumentException("Invalid bucket name");

			return $"{bucketName}.commits";
		}

		private static bool IsValidBucketName(string bucketName)
		{
			if (string.IsNullOrWhiteSpace(bucketName))
				return false;

			var regExp = new Regex("^[a-z]+$");
			return regExp.IsMatch(bucketName);
		}
	}
}
