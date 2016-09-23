using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NEStore.Aggregates;

namespace SampleMovieCatalog
{
	public class InMemoryMoviesProjection : ProjectionBase
	{
		private readonly Dictionary<Guid, MovieContract> _movies = new Dictionary<Guid, MovieContract>();

		public IEnumerable<MovieContract> Movies => _movies.Values;

		// ReSharper disable UnusedMember.Local
		private void On(Movie.Created @event)
		{
			if (!_movies.ContainsKey(@event.ObjectId))
				_movies.Add(@event.ObjectId,	new MovieContract {Id = @event.ObjectId});
		}
		private void On(Movie.TitleSet @event) => _movies[@event.ObjectId].Title = @event.Title;
		private void On(Movie.GenreSet @event) => _movies[@event.ObjectId].Genre = @event.Genre;
		// ReSharper restore UnusedMember.Local

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
