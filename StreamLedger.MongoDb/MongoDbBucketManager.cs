using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace StreamLedger.MongoDb
{
	public class MongoDbBucketManager : IBucketManager
	{
		public IMongoDatabase Database { get; }

		static MongoDbBucketManager()
		{
			BsonClassMap.RegisterClassMap<CommitData>(cm =>
			{
				cm.MapIdProperty(c => c.BucketRevision);
				cm.AutoMap();
				cm.SetIgnoreExtraElements(true);
			});
		}

		public MongoDbBucketManager(string connectionString)
		{
			var url = new MongoUrlBuilder(connectionString);

			if (string.IsNullOrWhiteSpace(url.DatabaseName))
				throw new ArgumentException("MongoDb connection string doesn't contain database");

			var client = new MongoClient(url.ToMongoUrl());

			Database = client.GetDatabase(url.DatabaseName);
		}

		public async Task EnsureBucketAsync(string bucketName)
		{
			var collection = CollectionFromBucket(bucketName);

			var builder = new IndexKeysDefinitionBuilder<CommitData>();

			await collection.Indexes.CreateManyAsync(new[]
			{
				new CreateIndexModel<CommitData>(builder
					.Ascending(p => p.StreamId)),
				new CreateIndexModel<CommitData>(builder
					.Ascending(p => p.StreamId)
					.Ascending(p => p.StreamRevisionStart), new CreateIndexOptions { Unique = true })
			});
		}

		public async Task DeleteBucketAsync(string bucketName)
		{
			await Database.DropCollectionAsync(CollectionNameFromBucket(bucketName));
		}

		public IBucket Bucket(string bucketName)
		{
			throw new NotImplementedException();
		}

		private string CollectionNameFromBucket(string bucketName)
		{
			if (!IsValidBucketName(bucketName))
				throw new ArgumentException("Invalid bucket name");

			return $"{bucketName}.commits";
		}
		private IMongoCollection<CommitData> CollectionFromBucket(string bucketName)
		{
			return Database.GetCollection<CommitData>(CollectionNameFromBucket(bucketName));
		}

		private bool IsValidBucketName(string bucketName)
		{
			if (string.IsNullOrWhiteSpace(bucketName))
				return false;

			var regExp = new Regex("^[a-z]+$");
			return regExp.IsMatch(bucketName);
		}
	}
}
