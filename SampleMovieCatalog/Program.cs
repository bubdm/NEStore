using System;
using System.Collections.Generic;
using System.Configuration;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using NEStore.Aggregates;
using NEStore.MongoDb;

namespace SampleMovieCatalog
{
	public static class Program
	{
		private static AggregateStore _store;
		private static MongoDbEventStore _eventStore;
		private static InMemoryMoviesProjection _moviesProjection;
		private static InMemoryTotalMoviesProjection _totalMoviesProjection;

		// TODO Implement retry logic

		public static void Main()
		{
			RegisterAllEvents();
			_eventStore = new MongoDbEventStore(ConfigurationManager.ConnectionStrings["mongoTest"].ConnectionString);
			_eventStore.RegisterDispatchers(
				_moviesProjection = new InMemoryMoviesProjection(),
				_totalMoviesProjection = new InMemoryTotalMoviesProjection());
			_store = new AggregateStore(_eventStore.Bucket("movies"));

			RebuildAsync().Wait();

			Console.WriteLine("Movie catalog sample!");

			while (true)
			{
				Console.WriteLine("");
				Console.WriteLine("Actions:");
				Console.WriteLine(" -i: Insert a movie");
				Console.WriteLine(" -u: Update movie");
				Console.WriteLine(" -l: List events");
				Console.WriteLine(" -p: Print movies");
				Console.WriteLine(" -t: Total movies");
				Console.WriteLine(" -m: Load movie aggregate");
				Console.WriteLine(" -r: Rollback");
				Console.WriteLine(" -e: Exit");
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
					case "r":
						RollbackAsync().Wait();
						break;
					case "p":
						PrintMovies();
						break;
					case "t":
						TotalMovies();
						break;
					case "e":
						return;
				}
			}
		}

		private static void TotalMovies()
		{
			Console.WriteLine($"Total movies: {_totalMoviesProjection.TotalMovies}");
		}

		private static void PrintMovies()
		{
			foreach (var m in _moviesProjection.Movies)
				ObjectDumper.Write(m);
		}

		private static async Task RebuildAsync()
		{
			foreach (ProjectionBase projection in _eventStore.GetDispatchers())
				await projection.ClearAsync();

			foreach (var e in await _store.GetEventsAsync())
				foreach (var projection in _eventStore.GetDispatchers())
					await projection.DispatchAsync(e);
		}

		private static async Task RollbackAsync()
		{
			Console.Write("BucketRevision: ");
			var revision = int.Parse(Console.ReadLine());

			await _store.RollbackAsync(revision);
			await RebuildAsync();
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
			if (movie.Title != title)
				movie.Title = title;

			Console.Write("Genre: ");
			var genre = Console.ReadLine();
			if (movie.Genre != genre)
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
