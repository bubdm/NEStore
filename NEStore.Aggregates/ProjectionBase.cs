using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace NEStore.Aggregates
{
	public abstract class ProjectionBase : IDispatcher<IEvent>
	{
		private readonly Dictionary<Type, MethodInfo> _eventHandlers;

		protected ProjectionBase()
		{
			_eventHandlers = GetType()
				.GetMethods(BindingFlags.Default | BindingFlags.NonPublic | BindingFlags.Instance)
				.Where(m => m.Name == "On" && m.GetParameters().Length == 1)
				.ToDictionary(m => m.GetParameters().Single().ParameterType, m => m);
		}

		public abstract Task ClearAsync();
		
		public Task DispatchAsync(string bucketName, CommitData<IEvent> commit)
		{
			var tasks = new List<Task>();

			foreach (var @event in commit.Events)
			{
				MethodInfo method;
				if (!_eventHandlers.TryGetValue(@event.GetType(), out method))
					continue;

				var task = method.Invoke(this, new object[] { @event }) as Task;

				if(task != null)
					tasks.Add(task);
			}
			return Task.WhenAll(tasks);
		}
	}
}
