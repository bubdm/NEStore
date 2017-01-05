using System;
using System.Threading.Tasks;
using MongoDB.Bson;
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
			using (var fixture = new MongoDbEventStoreFixture<object>())
			{
				await Assert.ThrowsAsync<ArgumentException>(() => fixture.EventStore.EnsureBucketAsync(bucketName));
			}
		}

		[Fact]
		public async Task EnsureBucket_create_required_collections()
		{
			using (var fixture = new MongoDbEventStoreFixture<object>())
			{
				await fixture.EventStore.EnsureBucketAsync(fixture.BucketName);

				var collections = await (await fixture.EventStore.Database.ListCollectionsAsync()).ToListAsync();

				Assert.Contains(collections, p => p["name"] == $"{fixture.BucketName}.commits");
			}
		}

		[Fact]
		public async Task Ensure_Bucket_delete()
		{
			using (var fixture = new MongoDbEventStoreFixture<object>())
			{
				await fixture.EventStore.EnsureBucketAsync(fixture.BucketName);

				var collections = await (await fixture.EventStore.Database.ListCollectionsAsync()).ToListAsync();

				Assert.Contains(collections, p => p["name"] == $"{fixture.BucketName}.commits");

				await fixture.EventStore.DeleteBucketAsync(fixture.BucketName);

				collections = await (await fixture.EventStore.Database.ListCollectionsAsync()).ToListAsync();

				Assert.DoesNotContain(collections, p => p["name"] == $"{fixture.BucketName}.commits");
			}
		}

		[Theory]
		[InlineData("mongodb://localhost", "majority", true, null, ReadPreferenceMode.Primary, GuidRepresentation.Standard)]
		[InlineData("mongodb://localhost?readConcernLevel=majority", "majority", true, ReadConcernLevel.Majority, ReadPreferenceMode.Primary, GuidRepresentation.Standard)]
		[InlineData("mongodb://localhost?readPreference=nearest", "majority", true, null, ReadPreferenceMode.Nearest, GuidRepresentation.Standard)]
		[InlineData("mongodb://localhost?w=3", "3", true, null, ReadPreferenceMode.Primary, GuidRepresentation.Standard)]
		[InlineData("mongodb://localhost?journal=false", "majority", false, null, ReadPreferenceMode.Primary, GuidRepresentation.Standard)]
		[InlineData("mongodb://localhost?uuidRepresentation=javaLegacy", "majority", true, null, ReadPreferenceMode.Primary, GuidRepresentation.JavaLegacy)]
		public void Get_database_settings(
			string connectionString, 
			string w, 
			bool journal, 
			ReadConcernLevel? readConcernLevel, 
			ReadPreferenceMode readPreference,
			GuidRepresentation guidRepresentation)
		{
			var settings = MongoDbEventStore<object>.GetDefaultDatabaseSettings(connectionString);

			Assert.Equal(w, settings.WriteConcern.W.ToString());
			Assert.Equal(journal, settings.WriteConcern.Journal);
			if (readConcernLevel != null)
				Assert.Equal(readConcernLevel, settings.ReadConcern.Level);

			Assert.Equal(readPreference, settings.ReadPreference.ReadPreferenceMode);
			Assert.Equal(guidRepresentation, settings.GuidRepresentation);
		}

	}
}