namespace NEStore.DomainObjects.Events
{
	public interface IEventHandler<in T>
		where T : IEvent
	{
		void On(T @event);
	}
}