using System.Threading.Tasks;

namespace NEStore
{
	public interface IDispatcher<T>
	{
		Task DispatchAsync(string bucketName, CommitData<T> commit);
	}
}