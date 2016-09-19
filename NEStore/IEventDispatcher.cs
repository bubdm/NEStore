using System.Threading.Tasks;

namespace NEStore
{
	public interface IEventDispatcher<T>
	{
		Task DispatchAsync(T @event);
	}
}