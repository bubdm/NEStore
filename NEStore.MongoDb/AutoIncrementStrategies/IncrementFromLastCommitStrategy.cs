using System.Threading;
using System.Threading.Tasks;

namespace NEStore.MongoDb.AutoIncrementStrategies
{
	public class IncrementFromLastCommitStrategy : IAutoIncrementStrategy
	{
		public Task<long> IncrementAsync(string bucketName, CommitInfo lastCommit, CancellationToken token = default)
		{
			return Task.FromResult(lastCommit?.BucketRevision + 1 ?? 1);
		}

		public Task RollbackAsync(string bucketName, long bucketRevision, CancellationToken token = default)
		{
			return Task.FromResult(false);
		}

		public Task DeleteBucketAsync(string bucketName, CancellationToken token = default)
		{
			return Task.FromResult(false);
		}
	}
}