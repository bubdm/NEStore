using System.Collections.Generic;
using System.Threading.Tasks;

namespace NEStore.Aggregates
{
	public abstract class ProjectionBase : IDispatcher<IEvent>
	{
		public abstract Task ClearAsync();

		public Task DispatchAsync(string bucketName, CommitData<IEvent> commit)
		{
			var tasks = new List<Task>();

			foreach (var @event in commit.Events)
			{
				var method = GetType()
					.GetMethod("On",
					System.Reflection.BindingFlags.Default | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance,
					null,
					new[] { @event.GetType() },
					null);

				var task = method?.Invoke(this, new[] { @event }) as Task;

				if(task != null)
					tasks.Add(task);
			}
			return Task.WhenAll(tasks);
		}
	}
}
