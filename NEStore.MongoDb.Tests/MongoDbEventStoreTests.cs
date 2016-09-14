using System;
using System.Threading.Tasks;
using MongoDB.Driver;
using Xunit;

namespace NEStore.MongoDb.Tests
{
	public class MongoDbEventStoreTests
	{
		[Theory]
		[InlineData(null)]
		[InlineData("")]
		[InlineData("test test")]
		[InlineData("Vsm")]
		[InlineData("test(")]
		[InlineData("5test")]
		public async Task Bucket_name_should_be_valid(string bucketName)
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				await Assert.ThrowsAsync<ArgumentException>(() => fixture.Target.EnsureBucketAsync(bucketName));
			}
		}

		[Fact]
		public async Task EnsureBucket_create_required_collections()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				await fixture.Target.EnsureBucketAsync(fixture.BucketName);

				var collections = await (await fixture.Target.Database.ListCollectionsAsync()).ToListAsync();

				Assert.Contains(collections, p => p["name"] == $"{fixture.BucketName}.commits");
			}
		}

		// TODO Test delete bucket
	}
}