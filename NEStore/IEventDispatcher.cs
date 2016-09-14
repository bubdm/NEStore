using System.Threading.Tasks;

namespace NEStore
{
	public interface IEventDispatcher
	{
		Task DispatchAsync(object @event);
	}
}