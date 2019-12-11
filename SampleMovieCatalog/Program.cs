using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Deltatre.CMS.Diagnostics;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson.Serialization.Conventions;
using Moq;
using NEStore;
using NEStore.DomainObjects.Aggregates;
using NEStore.DomainObjects.Events;
using NEStore.MongoDb;
using NEStore.MongoDb.AutoIncrementStrategies;
using NEStore.MongoDb.Conventions;
using Polly;
using SampleMovieCatalog.Helpers;
using SampleMovieCatalog.Movies;
using SampleMovieCatalog.Projections;

namespace SampleMovieCatalog
{
  public static class Program
  {
    private static AggregateStore _store;
    private static MongoDbEventStore<IEvent> _eventStore;
    private static InMemoryMoviesProjection _moviesProjection;
    private static InMemoryTotalMoviesProjection _totalMoviesProjection;

    private static readonly Policy _retryPolicy = Policy
      .Handle<Exception>(ex => ex is ConcurrencyWriteException || ex is UndispatchedEventsFoundException)
      .RetryForeverAsync(ex =>
      {
        Console.WriteLine($"Retry exception: {ex.Message}");
      });

    public static async Task Main()
    {
      SetupSerialization();

      var builder = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json");

      var configuration = builder.Build();

      var mongoDbConnectionString = configuration["mongoTest"];
      _eventStore = new MongoDbEventStore<IEvent>(mongoDbConnectionString, new Mock<ILogger>().Object);
      _eventStore.AutonIncrementStrategy = new IncrementCountersStrategy<IEvent>(_eventStore);
      _eventStore.RegisterDispatchers(
        _moviesProjection = new InMemoryMoviesProjection(),
        _totalMoviesProjection = new InMemoryTotalMoviesProjection());

      _store = new AggregateStore(_eventStore, "sample");

      await RebuildAsync().ConfigureAwait(false);

      Console.WriteLine("Movie catalog sample!");

      while (true)
      {
        Console.WriteLine("");
        Console.WriteLine("Actions:");
        Console.WriteLine(" -i: Insert a movie");
        Console.WriteLine(" -b: Bulk insert movies");
        Console.WriteLine(" -a: Parallel bulk insert movies");
        Console.WriteLine(" -u: Update movie");
        Console.WriteLine(" -l: List events");
        Console.WriteLine(" -p: Print movies");
        Console.WriteLine(" -t: Total movies");
        Console.WriteLine(" -m: Load movie aggregate");
        Console.WriteLine(" -r: Rollback");
        Console.WriteLine(" -s: Change auto-increment strategy");
        Console.WriteLine(" -e: Exit");
        Console.Write("Choose an action: ");
        switch (Console.ReadLine())
        {
          case "i":
            await InsertMovieAsync().ConfigureAwait(false);
            break;
          case "b":
            await BulkInsertMoviesAsync().ConfigureAwait(false);
            break;
          case "a":
            await ParallelBulkInsertMoviesAsync().ConfigureAwait(false);
            break;
          case "u":
            await UpdateMovieAsync().ConfigureAwait(false);
            break;
          case "l":
            await ListEventsAsync().ConfigureAwait(false);
            break;
          case "m":
            await LoadMovieAsync().ConfigureAwait(false);
            break;
          case "r":
            await RollbackAsync().ConfigureAwait(false);
            break;
          case "p":
            PrintMovies();
            break;
          case "t":
            TotalMovies();
            break;
          case "s":
            await ChangeStrategyAsync().ConfigureAwait(false);
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
      Console.WriteLine("Rebuilding projections...");
      var timer = new Stopwatch();
      timer.Start();

      await _store.RebuildAsync().ConfigureAwait(false);

      timer.Stop();
      Console.WriteLine("Rebuild time: " + timer.Elapsed);
    }

    private static async Task RollbackAsync()
    {
      Console.Write("BucketRevision: ");
      var revision = int.Parse(Console.ReadLine() ?? "");

      await _store.Bucket.RollbackAsync(revision).ConfigureAwait(false);
      await RebuildAsync().ConfigureAwait(false);
    }

    private static async Task LoadMovieAsync()
    {
      Console.Write("ObjectId: ");
      var id = Guid.Parse(Console.ReadLine() ?? "");

      var movie = await _store.LoadAsync<Movie>(id).ConfigureAwait(false);
      ObjectDumper.Write(movie);
    }

    private static async Task ListEventsAsync()
    {
      var events = await _store.Bucket.GetEventsAsync().ConfigureAwait(false);
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

    private static async Task BulkInsertMoviesAsync()
    {
      var timer = new Stopwatch();

      Console.Write("How many: ");
      var count = int.Parse(Console.ReadLine() ?? "1");

      timer.Start();

      await InsertMoviesAsync(0, count).ConfigureAwait(false);

      timer.Stop();
      Console.WriteLine("Elapsed: " + timer.Elapsed);
    }

    private static async Task ParallelBulkInsertMoviesAsync()
    {
      var timer = new Stopwatch();

      Console.Write("How many parallels: ");
      var parallels = int.Parse(Console.ReadLine() ?? "1");

      Console.Write("How many items: ");
      var count = int.Parse(Console.ReadLine() ?? "1");

      timer.Start();
      var tasks = new List<Task>();
      for (var i = 0; i < parallels; i++)
        tasks.Add(InsertMoviesAsync(i, count));

      await Task.WhenAll(tasks).ConfigureAwait(false);

      timer.Stop();
      Console.WriteLine("Elapsed: " + timer.Elapsed);
    }

    private static async Task InsertMoviesAsync(int iteration, int count)
    {
      var timer = new Stopwatch();
      timer.Start();
      for (var i = 0; i < count; i++)
      {
        var movie = new Movie(Guid.NewGuid())
        {
          Title = Guid.NewGuid().ToString(),
          Genre = Guid.NewGuid().ToString()
        };

        var result = await _retryPolicy.ExecuteAsync(() =>
           _store.SaveAsync(movie)
        ).ConfigureAwait(false);

        await result.DispatchTask.ConfigureAwait(false);
      }
      timer.Stop();
      Console.WriteLine($"Iteration {iteration} elapsed: {timer.Elapsed}");
    }

    private static async Task UpdateMovieAsync()
    {
      Console.Write("ObjectId: ");
      var id = Guid.Parse(Console.ReadLine() ?? "");
      var movie = await _store.LoadAsync<Movie>(id).ConfigureAwait(false);

      Console.Write("Title: ");
      var title = Console.ReadLine();
      if (movie.Title != title)
        movie.Title = title;

      Console.Write("Genre: ");
      var genre = Console.ReadLine();
      if (movie.Genre != genre)
        movie.Genre = genre;

      await _store.SaveAsync(movie).ConfigureAwait(false);
    }

    private static async Task ChangeStrategyAsync()
    {
      Console.WriteLine("Auto-increment strategies available:");
      Console.WriteLine("\t1) From Last commit");
      Console.WriteLine("\t2) From Counter");
      Console.Write("Strategy: ");
      var count = int.Parse(Console.ReadLine() ?? "1");

      if (count == 1)
        _eventStore.AutonIncrementStrategy = new IncrementFromLastCommitStrategy();

      if (count == 2)
        _eventStore.AutonIncrementStrategy = new IncrementCountersStrategy<IEvent>(_eventStore);

      await RebuildAsync().ConfigureAwait(false);
    }

    private static void SetupSerialization()
    {
      ConventionRegistry.Register(
        nameof(ImmutablePocoConvention),
        new ConventionPack { new ImmutablePocoConvention() },
        t => typeof(IEvent).IsAssignableFrom(t));

      var events = AppDomain.CurrentDomain.GetAssemblies()
        .SelectMany(p => p.GetTypes())
        .Where(p => typeof(IEvent).IsAssignableFrom(p) && p.IsClass && !p.IsAbstract);

      foreach (var e in events)
        MongoDbSerialization.Register(e);
    }

  }
}
