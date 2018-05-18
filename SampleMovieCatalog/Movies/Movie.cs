using System;
using System.Collections.Generic;
using NEStore.DomainObjects.Aggregates;
using NEStore.DomainObjects.Events;

namespace SampleMovieCatalog.Movies
{
	public class Movie :
		Aggregate,
		IEventHandler<MovieCreated>,
		IEventHandler<MovieTitleSet>,
		IEventHandler<MovieGenreSet>
	{
		private string _title;
		private string _genre;

		public Movie(IEnumerable<IEvent> events)
		{
			Inflate(events);
		}

		public Movie(Guid objectId)
		{
			ApplyChange(new MovieCreated(objectId));
		}

		public string Title
		{
			get => _title;
			set => ApplyChange(new MovieTitleSet(ObjectId, value));
		}

		public string Genre
		{
			get => _genre;
			set => ApplyChange(new MovieGenreSet(ObjectId, value));
		}

		public void On(MovieCreated @event) => ObjectId = @event.ObjectId;
		public void On(MovieTitleSet @event) => _title = @event.Title;
		public void On(MovieGenreSet @event) => _genre = @event.Genre;
	}
}
