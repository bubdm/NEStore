using System;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Moq;

namespace NEStore.MongoDb.Tests
{
	public class MongoDbEventStoreFixture : MongoDbEventStoreFixture<object>
	{
		public MongoDbEventStoreFixture(int dispatchDelay = 50) : base(dispatchDelay)
		{
		}
	}

	public class MongoDbEventStoreFixture<T> : IDisposable
	{
		public string BucketName { get; }
		public MongoDbEventStore<T> EventStore { get; }
		public MongoDbBucket<T> Bucket { get; }
		public Mock<IDispatcher<T>> Dispatcher { get; }

		public MongoDbEventStoreFixture(int dispatchDelay = 50)
		{
			BucketName = RandomString(10);
			EventStore = CreateTarget();
			Dispatcher = new Mock<IDispatcher<T>>();

			Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<string>(), It.IsAny<CommitData<T>>()))
				.Returns<string, CommitData<T>>((b, c) => Task.Delay(dispatchDelay));

			EventStore.RegisterDispatchers(Dispatcher.Object);
			Bucket = EventStore.Bucket(BucketName) as MongoDbBucket<T>;
		}

		public void Dispose()
		{
			CleanUp();
		}

		public void CleanUp()
		{
			EventStore.DeleteBucketAsync(BucketName).Wait();
		}

		private static MongoDbEventStore<T> CreateTarget()
		{
			var cns = ConfigurationManager.ConnectionStrings["mongoTest"].ConnectionString;

			return new MongoDbEventStore<T>(cns);
		}

		private static string RandomString(int length)
		{
			const string chars = "abcdefghijklmnopqrstuvwxyz";
			var random = new Random();
			return new string(Enumerable.Repeat(chars, length)
				.Select(s => s[random.Next(s.Length)]).ToArray());
		}
	}
}