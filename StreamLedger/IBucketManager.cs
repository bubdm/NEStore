using System.Threading.Tasks;

namespace StreamLedger
{
	public interface IBucketManager
	{
		Task EnsureBucketAsync(string bucketName);
		Task DeleteBucketAsync(string bucketName);

		IBucket Bucket(string bucketName);
	}
}
