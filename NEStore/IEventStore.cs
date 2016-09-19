using System.Collections.Generic;
using System.Threading.Tasks;

namespace NEStore
{
	public interface IEventStore<T>
	{
		Task EnsureBucketAsync(string bucketName);

		Task DeleteBucketAsync(string bucketName);

		IBucket<T> Bucket(string bucketName);

		void RegisterDispatchers(params IDispatcher<T>[] dispatchers);
		
		IEnumerable<IDispatcher<T>> GetDispatchers();
	}
}