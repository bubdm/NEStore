using System;
using System.Collections.Generic;
using NEStore.DomainObjects.Events;

namespace NEStore.DomainObjects.Aggregates
{
	public abstract class Aggregate
	{
		private readonly EventDispatcher _dispatcher;
		private readonly List<IEvent> _changes = new List<IEvent>();

		public Guid ObjectId { get; protected set; }
		public int Version { get; private set; }

		protected Aggregate()
		{
			_dispatcher = new EventDispatcher(this);
		}

		protected void Inflate(IEnumerable<IEvent> events)
		{
			foreach (var e in events)
				OnEvent(e);
		}

		protected void ApplyChange(IEvent @event)
		{
			OnEvent(@event);
			_changes.Add(@event);
		}

		private void OnEvent(IEvent @event)
		{
			var invoked = _dispatcher.TryInvoke(@event);
			if (!invoked)
				throw new Exception($"Method 'On({@event.GetType().Name})' not found ");

			Version++;
		}

		public IEnumerable<IEvent> GetChanges()
		{
			return _changes.ToArray();
		}

		public void ClearChanges()
		{
			_changes.Clear();
		}
	}
}