using System.Threading.Tasks;

namespace NEStore.DomainObjects
{
	public interface IProjection : IDispatcher<IEvent>
	{
		Task ClearAsync();
	}
}