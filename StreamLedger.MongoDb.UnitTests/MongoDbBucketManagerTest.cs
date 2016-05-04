using System;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;
using Xunit;

namespace StreamLedger.MongoDb.UnitTests
{
	public class MongoDbBucketManagerTest : IDisposable
	{
		private readonly string _bucketName;
		private readonly MongoDbBucketManager _target;

		public MongoDbBucketManagerTest()
		{
			_bucketName = RandomString(10);
			_target = CreateTarget();
		}

		public void Dispose()
		{
			_target.DeleteBucketAsync(_bucketName).Wait();
		}

		[Theory]
		[InlineData(null)]
		[InlineData("")]
		[InlineData("test test")]
		[InlineData("Vsm")]
		[InlineData("test(")]
		[InlineData("5test")]
		public void Bucket_name_should_be_valid(string bucketName)
		{
			var target = CreateTarget();

			Assert.ThrowsAsync<ArgumentException>(() => target.EnsureBucketAsync(bucketName)).Wait();
		}

		[Fact]
		public async Task EnsureBucket_create_required_collections()
		{
			await _target.EnsureBucketAsync(_bucketName);

			var collections = await (await _target.Database.ListCollectionsAsync()).ToListAsync();

			Assert.Contains(collections, p => p["name"] == $"{_bucketName}.commits");
		}

		private static MongoDbBucketManager CreateTarget()
		{
			var cns = ConfigurationManager.ConnectionStrings["mongoTest"].ConnectionString;

			return new MongoDbBucketManager(cns);
		}

		public static string RandomString(int length)
		{
			const string chars = "abcdefghijklmnopqrstuvwxyz";
			var random = new Random();
			return new string(Enumerable.Repeat(chars, length)
				.Select(s => s[random.Next(s.Length)]).ToArray());
		}
	}
}