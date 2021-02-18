using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace NEStore.MongoDb.AutoIncrementStrategies
{
	public class IncrementCountersStrategy<T> : IAutoIncrementStrategy
	{
		private readonly IMongoCollection<Counter> _collection;
		
		public IncrementCountersStrategy(MongoDbEventStore<T> eventStore)
		{
			_collection = eventStore.Database.GetCollection<Counter>("counters");
		}

		public async Task<long> IncrementAsync(string bucketName, CommitInfo lastCommit, CancellationToken token = default)
		{
			var update = Builders<Counter>.Update.Inc(x => x.BucketRevision, 1);
			var projection = Builders<Counter>.Projection.Expression(x => x.BucketRevision);
			var options = new FindOneAndUpdateOptions<Counter,long>
			{
				Projection = projection,
				IsUpsert = false,
				ReturnDocument = ReturnDocument.After
			};

			var updatedCounter = await _collection.FindOneAndUpdateAsync(
				prop => prop.BucketName == bucketName,
				update,
				options, token).ConfigureAwait(false);

			if (updatedCounter != 0) return updatedCounter;

			await CreateCounterAsync(new Counter
			{
				BucketName = bucketName,
				BucketRevision = lastCommit?.BucketRevision ?? 0
			}).ConfigureAwait(false);

			return await IncrementAsync(bucketName, lastCommit).ConfigureAwait(false);
		}

		public async Task RollbackAsync(string bucketName, long bucketRevision, CancellationToken token = default)
		{
			var update = Builders<Counter>.Update.Set(x => x.BucketRevision, bucketRevision);

			await _collection.UpdateOneAsync(prop => prop.BucketName == bucketName, update, cancellationToken: token).ConfigureAwait(false);
		}

		public async Task DeleteBucketAsync(string bucketName, CancellationToken token = default)
		{
			await _collection.DeleteOneAsync(prop => prop.BucketName == bucketName, token).ConfigureAwait(false);
		}


		private async Task CreateCounterAsync(Counter counter)
		{
			try
			{
				await _collection.InsertOneAsync(counter).ConfigureAwait(false);
			}
			catch (MongoWriteException ex) when (ex.IsDuplicateKeyException())
			{
			}
		}
	}
}