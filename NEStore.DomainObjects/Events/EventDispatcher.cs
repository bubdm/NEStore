using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace NEStore.DomainObjects.Events
{
	public class EventDispatcher
	{
		private readonly object _target;
		private readonly Dictionary<Type, MethodInfo> _eventHandlers;

		public EventDispatcher(object target)
		{
			_target = target;

			_eventHandlers = _target.GetType()
				.GetInterfaces()
				.Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEventHandler<>))
				.ToDictionary(
					i => i.GetGenericArguments().Single(), i => i.GetMethod(nameof(IEventHandler<IEvent>.On)));
		}

		public bool TryInvoke(IEvent @event)
		{
			MethodInfo method;
			if (!_eventHandlers.TryGetValue(@event.GetType(), out method))
				return false;

			method.Invoke(_target, new object[] { @event });

			return true;
		}
	}
}