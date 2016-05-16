using System.Threading.Tasks;

namespace StreamLedger
{
	public interface IEventDispatcher
	{
		Task DispatchAsync(object @event);
	}
}