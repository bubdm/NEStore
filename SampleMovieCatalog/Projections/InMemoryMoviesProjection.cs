using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NEStore.DomainObjects.Events;
using NEStore.DomainObjects.Projections;
using SampleMovieCatalog.Movies;

namespace SampleMovieCatalog.Projections
{
	public class InMemoryMoviesProjection : 
		ProjectionBase,
		IEventHandler<MovieCreated>,
		IEventHandler<MovieTitleSet>,
		IEventHandler<MovieGenreSet>
	{
		private readonly Dictionary<Guid, MovieContract> _movies = new Dictionary<Guid, MovieContract>();

		public IEnumerable<MovieContract> Movies => _movies.Values;

		public void On(MovieCreated @event)
		{
			if (!_movies.ContainsKey(@event.ObjectId))
				_movies.Add(@event.ObjectId,	new MovieContract {Id = @event.ObjectId});
		}
		public void On(MovieTitleSet @event) => _movies[@event.ObjectId].Title = @event.Title;
		public void On(MovieGenreSet @event) => _movies[@event.ObjectId].Genre = @event.Genre;

		public override Task ClearAsync()
		{
			_movies.Clear();
			return Task.FromResult(false);
		}
	}

	public class MovieContract
	{
		public Guid Id { get; set; }
		public string Title { get; set; }
		public string Genre { get; set; }
	}
}
