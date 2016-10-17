using System.Threading.Tasks;
using NEStore.DomainObjects.Events;

namespace NEStore.DomainObjects.Projections
{
	public interface IProjection : IDispatcher<IEvent>
	{
		Task ClearAsync();
	}
}