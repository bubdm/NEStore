using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NEStore.Aggregates;

namespace SampleMovieCatalog
{
	public class InMemoryMoviesProjection : ProjectionBase
	{
		public List<MovieContract> Movies { get; set; } = new List<MovieContract>();

		// ReSharper disable UnusedMember.Local
        // TODO This should be an AddOrUpdate
		private void On(Movie.Created @event) => Movies.Add(new MovieContract { Id = @event.ObjectId});
		private void On(Movie.TitleSet @event) => Movies.Find(p => p.Id == @event.ObjectId).Title = @event.Title;
		private void On(Movie.GenreSet @event) => Movies.Find(p => p.Id == @event.ObjectId).Genre = @event.Genre;
		// ReSharper restore UnusedMember.Local

		public override Task ClearAsync()
		{
			Movies.Clear();
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
