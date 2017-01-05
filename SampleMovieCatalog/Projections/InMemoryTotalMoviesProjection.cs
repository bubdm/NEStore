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
		private readonly HashSet<Guid> _movies = new HashSet<Guid>();
		public int TotalMovies
		{
			get
			{
				lock (_movies)
				{
					return _movies.Count;
				}
			}
		}

		public void On(MovieCreated @event)
		{
			lock (_movies)
			{
				if (!_movies.Contains(@event.ObjectId))
					_movies.Add(@event.ObjectId);
			}
		}

		public override Task ClearAsync()
		{
			lock (_movies)
			{
				_movies.Clear();
				return Task.FromResult(false);
			}
		}
	}
}
