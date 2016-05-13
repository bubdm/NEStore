using System.Threading.Tasks;

namespace StreamLedger
{
	public interface IEventDispatcher
	{
		Task Dispatch(object @event);
	}
}