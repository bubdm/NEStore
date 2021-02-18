using System.Threading;
using System.Threading.Tasks;

namespace NEStore.MongoDb.AutoIncrementStrategies
{
	public interface IAutoIncrementStrategy
	{
		Task<long> IncrementAsync(string bucketName, CommitInfo lastCommit, CancellationToken token = default);

		Task RollbackAsync(string bucketName, long bucketRevision, CancellationToken token = default);

		Task DeleteBucketAsync(string bucketName, CancellationToken token = default);
	}
}
