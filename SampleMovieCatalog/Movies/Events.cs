using System;
using NEStore.DomainObjects.Events;

namespace SampleMovieCatalog.Movies
{
	public class MovieCreated : IEvent
	{
		public MovieCreated(Guid objectId)
		{
			ObjectId = objectId;
		}

		public Guid ObjectId { get; }
	}
	public class MovieTitleSet : IEvent
	{
		public MovieTitleSet(Guid objectId, string title)
		{
			ObjectId = objectId;
			Title = title;
		}

		public Guid ObjectId { get; }
		public string Title { get; }
	}
	public class MovieGenreSet : IEvent
	{
		public MovieGenreSet(Guid objectId, string genre)
		{
			ObjectId = objectId;
			Genre = genre;
		}

		public Guid ObjectId { get; }
		public string Genre { get; }
	}
}
