using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace StreamLedger.MongoDb.UnitTests
{
	public class MongoDbBucketTests : IClassFixture<MongoDbLedgerFixture>
	{
		private readonly MongoDbLedgerFixture _fixture;
		private readonly MongoDbBucket _target;

		public MongoDbBucketTests(MongoDbLedgerFixture fixture)
		{
			_fixture = fixture;
			_target = (MongoDbBucket)fixture.Target.Bucket(fixture.BucketName);
		}

		[Fact]
		public async Task Query_empty_collections()
		{
			Assert.Equal(0, (await _target.GetBucketRevisionAsync()));
			Assert.Equal(0, (await _target.GetStreamRevisionAsync(Guid.NewGuid())));
			Assert.Equal(0, (await _target.GetStreamIdsAsync()).Count());
		}

		[Fact]
		public async Task Write_an_event()
		{
			var streamId = Guid.NewGuid();

			await _target.WriteAsync(streamId, 0, new [] { new {n1 = "v1" } });

			Assert.Equal(1, (await _target.GetBucketRevisionAsync()));
			Assert.Equal(streamId, (await _target.GetStreamIdsAsync()).Single());
			Assert.Equal(1, (await _target.GetStreamRevisionAsync(streamId)));

			var storedEvents = await _target.GetEventsAsync(streamId);
			Assert.Equal("v1", ((dynamic)storedEvents.Single()).n1);

			var commits = await _target.GetCommitsAsync(0, 1);
			Assert.Equal(1, commits.Count());

			var ids = await _target.GetStreamIdsAsync();
			Assert.Equal(streamId, ids.Single());

			Assert.Equal(false, await _target.HasUndispatchedCommitsAsync());

			_fixture.CleanUp();
		}
	}
}