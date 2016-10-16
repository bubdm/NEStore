using System;
using System.Collections.Generic;
using System.Dynamic;
using NEStore.DomainObjects;

namespace SampleMovieCatalog
{
	public partial class Movie : Aggregate
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

		// ReSharper disable UnusedMember.Local
		private void On(Created @event) => ObjectId = @event.ObjectId;
		private void On(TitleSet @event) => _title = @event.Title;
		private void On(GenreSet @event) => _genre = @event.Genre;
		private void On(ExtendedFieldsSet @event) => _extendedFields = @event.Fields;
		// ReSharper restore UnusedMember.Local
	}
}
