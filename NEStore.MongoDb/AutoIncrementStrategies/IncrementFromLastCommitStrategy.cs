using System.Threading.Tasks;

namespace NEStore.MongoDb.AutoIncrementStrategies
{
	public class IncrementFromLastCommitStrategy : IAutoIncrementStrategy
	{
		public Task<long> IncrementAsync(string bucketName, CommitInfo lastCommit)
		{
			return Task.FromResult(lastCommit?.BucketRevision + 1 ?? 1);
		}

		public Task RollbackAsync(string bucketName, long bucketRevision)
		{
			return Task.FromResult(false);
		}

		public Task DeleteBucketAsync(string bucketName)
		{
			return Task.FromResult(false);
		}
	}
}