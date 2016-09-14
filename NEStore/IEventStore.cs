using System.Collections.Generic;
using System.Threading.Tasks;

namespace NEStore
{
	public interface IEventStore
	{
		Task EnsureBucketAsync(string bucketName);

		Task DeleteBucketAsync(string bucketName);

		IBucket Bucket(string bucketName);

		void RegisterDispatchers(params IEventDispatcher[] dispatchers);

		IEnumerable<IEventDispatcher> GetDispatchers();
	}
}