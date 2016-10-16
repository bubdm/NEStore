using System;
using System.Dynamic;
using NEStore.DomainObjects;

namespace SampleMovieCatalog
{
	public partial class Movie
	{
		public class Created : IEvent
		{
			public Created(Guid objectId)
			{
				ObjectId = objectId;
			}

			public Guid ObjectId { get; }
		}
		public class TitleSet : IEvent
		{
			public TitleSet(Guid objectId, string title)
			{
				ObjectId = objectId;
				Title = title;
			}

			public Guid ObjectId { get; }
			public string Title { get; }
		}
		public class GenreSet : IEvent
		{
			public GenreSet(Guid objectId, string genre)
			{
				ObjectId = objectId;
				Genre = genre;
			}

			public Guid ObjectId { get; }
			public string Genre { get; }
		}
		public class ExtendedFieldsSet : IEvent
		{
			public ExtendedFieldsSet(Guid objectId, ExpandoObject fields)
			{
				ObjectId = objectId;
				Fields = fields;
			}

			public Guid ObjectId { get; }
			public ExpandoObject Fields { get; }
		}

	}
}
