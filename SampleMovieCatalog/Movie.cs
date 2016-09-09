using System;
using System.Collections.Generic;
using System.Dynamic;
using StreamLedger.Aggregates;

namespace SampleMovieCatalog
{
	public class Movie : Aggregate
	{
		private string _title;
		private string _genre;
		private ExpandoObject _extendedFields;

		public Movie(IEnumerable<IEvent> events)
		{
			Inflate(events);
		}

		public Movie(Guid objectId)
		{
			ApplyChange(new Created(objectId));
		}

		public string Title
		{
			get { return _title; }
			set { ApplyChange(new TitleSet(ObjectId, value)); }
		}

		public string Genre
		{
			get { return _genre; }
			set { ApplyChange(new GenreSet(ObjectId, value)); }
		}

		public ExpandoObject ExtendedFields
		{
			get { return _extendedFields; }
			set { ApplyChange(new ExtendedFieldsSet(ObjectId, value)); }
		}

		// ReSharper disable once UnusedMember.Local
		private void On(Created @event) => ObjectId = @event.ObjectId;
		// ReSharper disable once UnusedMember.Local
		private void On(TitleSet @event) => _title = @event.Title;
		// ReSharper disable once UnusedMember.Local
		private void On(GenreSet @event) => _genre = @event.Genre;
		// ReSharper disable once UnusedMember.Local
		private void On(ExtendedFieldsSet @event) => _extendedFields = @event.Fields;

		public class Created : IEvent
		{
			public Created(Guid objectId)
			{
				ObjectId = objectId;
			}

			public Guid ObjectId { get; private set; }
		}
		public class TitleSet : IEvent
		{
			public TitleSet(Guid objectId, string title)
			{
				ObjectId = objectId;
				Title = title;
			}

			public Guid ObjectId { get; private set; }
			public string Title { get; private set; }
		}
		public class GenreSet : IEvent
		{
			public GenreSet(Guid objectId, string genre)
			{
				ObjectId = objectId;
				Genre = genre;
			}

			public Guid ObjectId { get; private set; }
			public string Genre { get; private set; }
		}

		public class ExtendedFieldsSet : IEvent
		{
			public ExtendedFieldsSet(Guid objectId, ExpandoObject fields)
			{
				ObjectId = objectId;
				Fields = fields;
			}

			public Guid ObjectId { get; private set; }
			public ExpandoObject Fields { get; private set; }
		}
	}
}
