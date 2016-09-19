using System.Threading.Tasks;

namespace NEStore.Aggregates
{
	public abstract class ProjectionBase : IEventDispatcher<IEvent>
	{
		public abstract Task ClearAsync();

		public Task DispatchAsync(IEvent @event)
		{
			var method = GetType()
				.GetMethod("On",
				System.Reflection.BindingFlags.Default | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance,
				null,
				new[] { @event.GetType() },
				null);

			if (method == null)
				return Task.FromResult<object>(null);

			var task = method.Invoke(this, new[] { @event }) as Task;

			if (task != null)
				return task;

			return Task.FromResult<object>(null);
		}
	}
}
