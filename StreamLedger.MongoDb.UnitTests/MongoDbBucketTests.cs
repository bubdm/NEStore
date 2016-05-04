using System;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;
using Xunit;

namespace StreamLedger.MongoDb.UnitTests
{
	public class MongoDbBucketTests : IClassFixture<BucketManagerFixture>
	{
		private readonly MongoDbBucket _target;

		public MongoDbBucketTests(BucketManagerFixture fixture)
		{
			_target = (MongoDbBucket)fixture.Target.Bucket(fixture.BucketName);
		}

		[Fact]
		public async Task Query_empty_collections()
		{
			Assert.Equal(0, (await _target.BucketRevisionAsync()));
			Assert.Equal(0, (await _target.StreamIdsAsync()).Count());
			Assert.Equal(0, (await _target.StreamRevisionAsync(Guid.NewGuid())));
		}

		[Fact]
		public async Task Write_a_primitive_event()
		{
			var streamId = Guid.NewGuid();

			await _target.WriteAsync(streamId, 0, new [] { "event1" });

			Assert.Equal(1, (await _target.BucketRevisionAsync()));
			Assert.Equal(streamId, (await _target.StreamIdsAsync()).Single());
			Assert.Equal(1, (await _target.StreamRevisionAsync(streamId)));

			var storedEvents = await _target.EventsAsync(streamId);
			Assert.Equal("event1", storedEvents.Single());
		}
	}
}