using System.Threading.Tasks;
using NEStore.DomainObjects.Events;

namespace NEStore.DomainObjects.Projections
{
	public abstract class ProjectionBase : IProjection
	{
		private readonly EventDispatcher _dispatcher;

		protected ProjectionBase()
		{
			_dispatcher = new EventDispatcher(this);
		}

		public abstract Task ClearAsync();
		
		public Task DispatchAsync(string bucketName, CommitData<IEvent> commit)
		{
			foreach (var @event in commit.Events)
			{
				_dispatcher.TryInvoke(@event);
			}

			return Task.FromResult(true);
		}
	}
}
