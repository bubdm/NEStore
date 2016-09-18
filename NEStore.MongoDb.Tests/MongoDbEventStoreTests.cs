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
				await Assert.ThrowsAsync<ArgumentException>(() => fixture.EventStore.EnsureBucketAsync(bucketName));
			}
		}

		[Fact]
		public async Task EnsureBucket_create_required_collections()
		{
			using (var fixture = new MongoDbEventStoreFixture())
			{
				await fixture.EventStore.EnsureBucketAsync(fixture.BucketName);

				var collections = await (await fixture.EventStore.Database.ListCollectionsAsync()).ToListAsync();

				Assert.Contains(collections, p => p["name"] == $"{fixture.BucketName}.commits");
			}
		}

        [Fact]
        public async Task Ensure_Bucket_delete()
        {
            using (var fixture = new MongoDbEventStoreFixture())
            {
                await fixture.EventStore.EnsureBucketAsync(fixture.BucketName);

                var collections = await (await fixture.EventStore.Database.ListCollectionsAsync()).ToListAsync();

                Assert.Contains(collections, p => p["name"] == $"{fixture.BucketName}.commits");

                await fixture.EventStore.DeleteBucketAsync(fixture.BucketName);

                collections = await (await fixture.EventStore.Database.ListCollectionsAsync()).ToListAsync();

                Assert.DoesNotContain(collections, p => p["name"] == $"{fixture.BucketName}.commits");
            }
        }
    }
}