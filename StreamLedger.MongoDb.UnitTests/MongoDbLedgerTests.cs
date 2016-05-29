using System;
using System.Threading.Tasks;
using MongoDB.Driver;
using Xunit;

namespace StreamLedger.MongoDb.UnitTests
{
	public class MongoDbLedgerTests
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
			using (var fixture = new MongoDbLedgerFixture())
			{
				await Assert.ThrowsAsync<ArgumentException>(() => fixture.Target.EnsureBucketAsync(bucketName));
			}
		}

		[Fact]
		public async Task EnsureBucket_create_required_collections()
		{
			using (var fixture = new MongoDbLedgerFixture())
			{
				await fixture.Target.EnsureBucketAsync(fixture.BucketName);

				var collections = await (await fixture.Target.Database.ListCollectionsAsync()).ToListAsync();

				Assert.Contains(collections, p => p["name"] == $"{fixture.BucketName}.commits");
			}
		}
	}
}