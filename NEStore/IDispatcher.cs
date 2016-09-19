using System.Threading.Tasks;

namespace NEStore
{
	public interface IDispatcher<T>
	{
		Task DispatchAsync(CommitData<T> commit);
	}
}