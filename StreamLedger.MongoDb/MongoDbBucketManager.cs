using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace StreamLedger.MongoDb
{
	public class MongoDbBucketManager : IBucketManager
	{
		private readonly IMongoDatabase _database;

		public MongoDbBucketManager(string connectionString)
		{
			var url = new MongoUrlBuilder(connectionString);

			if (string.IsNullOrWhiteSpace(url.DatabaseName))
				throw new ArgumentException("MongoDb connection string doesn't contain database");

			var client = new MongoClient(url.ToMongoUrl());

			_database = client.GetDatabase(url.DatabaseName);
		}

		public async Task EnsureBucketAsync(string bucketName)
		{
			CollectionFromBucket(bucketName);
		}

		public Task DeleteBucketAsync(string bucketName)
		{
			throw new NotImplementedException();
		}

		public IBucket Bucket(string bucketName)
		{
			throw new NotImplementedException();
		}

		private IMongoCollection<BsonDocument> CollectionFromBucket(string bucketName)
		{
			if (!IsValidBucketName(bucketName))
				throw new ArgumentException("Invalid bucket name");

			return _database.GetCollection<BsonDocument>(bucketName);
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
