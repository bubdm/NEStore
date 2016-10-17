using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NEStore.DomainObjects.Events;
using NEStore.DomainObjects.Projections;
using SampleMovieCatalog.Movies;

namespace SampleMovieCatalog.Projections
{
	public class InMemoryTotalMoviesProjection :
		ProjectionBase,
		IEventHandler<MovieCreated>
	{
		public int TotalMovies => Movies.Count;

		public HashSet<Guid> Movies { get; set; } = new HashSet<Guid>();

		public void On(MovieCreated @event)
		{
			if (!Movies.Contains(@event.ObjectId))
				Movies.Add(@event.ObjectId);
		}

		public override Task ClearAsync()
		{
			Movies.Clear();
			return Task.FromResult(false);
		}
	}
}
