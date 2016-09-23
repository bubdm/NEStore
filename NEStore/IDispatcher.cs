using System.Threading.Tasks;

namespace NEStore
{
	public interface IDispatcher<T>
	{
		/// <summary>
		/// Dispatch a commit
		/// </summary>
		/// <param name="commit">Commit to dispatch</param>
		Task DispatchAsync(CommitData<T> commit);
	}
}