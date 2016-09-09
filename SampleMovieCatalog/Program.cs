using System;
using System.Collections.Generic;
using System.Configuration;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using StreamLedger.Aggregates;
using StreamLedger.MongoDb;

namespace SampleMovieCatalog
{
	public static class Program
	{
		private static AggregateStore _store;

		public static void Main()
		{
			RegisterAllEvents();
			var ledger = new MongoDbLedger(ConfigurationManager.ConnectionStrings["mongoTest"].ConnectionString);
			_store = new AggregateStore(ledger.Bucket("movies"));

			Console.WriteLine("Movie catalog sample!");

			while (true)
			{
				Console.WriteLine("Actions:");
				Console.WriteLine("-i: Insert a movie");
				Console.WriteLine("-u: Update movie");
				Console.WriteLine("-l: List events");
				Console.WriteLine("-m: Load movie");
				Console.WriteLine("-e: Exit");
				Console.Write("Choose an action: ");
				switch (Console.ReadLine())
				{
					case "i":
						InsertMovieAsync().Wait();
						break;
					case "u":
						UpdateMovieAsync().Wait();
						break;
					case "l":
						ListEventsAsync().Wait();
						break;
					case "m":
						LoadMovieAsync().Wait();
						break;
					case "e":
						return;
				}
			}
		}

		private static async Task LoadMovieAsync()
		{
			Console.Write("ObjectId: ");
			var id = Guid.Parse(Console.ReadLine());

			var movie = await _store.LoadAsync<Movie>(id);
			ObjectDumper.Write(movie);
		}

		private static async Task ListEventsAsync()
		{
			var events = await _store.GetEventsAsync();
			foreach (var e in events)
				ObjectDumper.Write(e);
		}

		private static Task InsertMovieAsync()
		{
			var movie = new Movie(Guid.NewGuid());

			Console.Write("Title: ");
			movie.Title = Console.ReadLine();
			Console.Write("Genre: ");
			movie.Genre = Console.ReadLine();

			return _store.SaveAsync(movie);
		}

		private static async Task UpdateMovieAsync()
		{
			Console.Write("ObjectId: ");
			var id = Guid.Parse(Console.ReadLine());
			var movie = await _store.LoadAsync<Movie>(id);

			Console.Write("Title: ");
			var title = Console.ReadLine();
			if (string.IsNullOrWhiteSpace(title))
				movie.Title = title;

			Console.Write("Genre: ");
			var genre = Console.ReadLine();
			if (string.IsNullOrWhiteSpace(genre))
				movie.Genre = genre;

			Console.Write("ExtendedFields: ");
			var fields = new ExpandoObject();
			while (true)
			{
				Console.Write("Field name: ");
				var fieldName = Console.ReadLine();
				if (string.IsNullOrWhiteSpace(fieldName))
					break;
				Console.Write("Field value: ");
				var fieldValue = Console.ReadLine();

				((IDictionary<string, object>) fields)[fieldName] = fieldValue;
			}
			movie.ExtendedFields = fields;

			await _store.SaveAsync(movie);
		}

		private static void RegisterAllEvents()
		{
			var events = AppDomain.CurrentDomain.GetAssemblies()
				.SelectMany(p => p.GetTypes())
				.Where(p => typeof(IEvent).IsAssignableFrom(p) && p.IsClass && !p.IsAbstract);

			foreach (var e in events)
				MongoDbSerialization.Register(e);
		}

	}
}
