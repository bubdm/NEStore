using System;
using System.Collections.Generic;

namespace NEStore.Aggregates
{
	public abstract class Aggregate
	{
		private readonly List<IEvent> _changes = new List<IEvent>();

		public Guid ObjectId { get; protected set; }
		public int Version { get; private set; }

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
			var method = GetType()
				.GetMethod("On", 
				System.Reflection.BindingFlags.Default | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, 
				null, 
				new[] { @event.GetType() },
				null);

			if (method == null)
				throw new Exception($"Method 'On({@event.GetType().Name})' not found ");

			method.Invoke(this, new [] { @event });
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