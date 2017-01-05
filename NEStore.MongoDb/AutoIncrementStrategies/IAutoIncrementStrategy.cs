using System.Threading.Tasks;

namespace NEStore.MongoDb.AutoIncrementStrategies
{
	public interface IAutoIncrementStrategy
	{
		Task<long> IncrementAsync(string bucketName, CommitInfo lastCommit);

		Task RollbackAsync(string bucketName, long bucketRevision);

		Task DeleteBucketAsync(string bucketName);
	}
}
