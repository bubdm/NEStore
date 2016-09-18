using System;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Moq;

namespace NEStore.MongoDb.Tests
{
	public class MongoDbEventStoreFixture : IDisposable
	{
		public string BucketName { get; }
		public MongoDbEventStore EventStore { get; }
		public MongoDbBucket Bucket { get; }
		public Mock<IEventDispatcher> Dispatcher { get; }

		public MongoDbEventStoreFixture()
		{
			BucketName = RandomString(10);
			EventStore = CreateTarget();
			Dispatcher = new Mock<IEventDispatcher>();

			Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<object>()))
				.Returns<object>(e => Task.Delay(200));

			EventStore.RegisterDispatchers(Dispatcher.Object);
			Bucket = EventStore.Bucket(BucketName) as MongoDbBucket;
		}

		public void Dispose()
		{
			CleanUp();
		}

		public void CleanUp()
		{
			EventStore.DeleteBucketAsync(BucketName).Wait();
		}

		private static MongoDbEventStore CreateTarget()
		{
			var cns = ConfigurationManager.ConnectionStrings["mongoTest"].ConnectionString;

			return new MongoDbEventStore(cns);
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