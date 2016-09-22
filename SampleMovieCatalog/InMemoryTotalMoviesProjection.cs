using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NEStore.Aggregates;

namespace SampleMovieCatalog
{
	public class InMemoryTotalMoviesProjection : ProjectionBase
	{
		public int TotalMovies => Movies.Count;

		public HashSet<Guid> Movies { get; set; } = new HashSet<Guid>();

		// ReSharper disable UnusedMember.Local
		private void On(Movie.Created @event)
		{
			if (!Movies.Contains(@event.ObjectId))
				Movies.Add(@event.ObjectId);
		}
		// ReSharper restore UnusedMember.Local

		public override Task ClearAsync()
		{
			Movies.Clear();
			return Task.FromResult(false);
		}
	}
}
