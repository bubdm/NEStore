using System.Threading;
using System.Threading.Tasks;

namespace NEStore
{
	public interface IDispatcher<T>
	{
		/// <summary>
		/// Dispatch a commit
		/// </summary>
		/// /// <param name="bucketName">Bucket identifier</param>
		/// <param name="commit">Commit to dispatch</param>
		/// <param name="token">The Cancellation Token</param>
		Task DispatchAsync(string bucketName, CommitData<T> commit, CancellationToken token = default);
	}
}