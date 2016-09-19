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
		public MongoDbEventStore<object> EventStore { get; }
		public MongoDbBucket<object> Bucket { get; }
		public Mock<IDispatcher<object>> Dispatcher { get; }

		public MongoDbEventStoreFixture()
		{
			BucketName = RandomString(10);
			EventStore = CreateTarget();
			Dispatcher = new Mock<IDispatcher<object>>();

			Dispatcher.Setup(p => p.DispatchAsync(It.IsAny<object>()))
				.Returns<object>(e => Task.Delay(50));

			EventStore.RegisterDispatchers(Dispatcher.Object);
			Bucket = EventStore.Bucket(BucketName) as MongoDbBucket<object>;
		}

		public void Dispose()
		{
			CleanUp();
		}

		public void CleanUp()
		{
			EventStore.DeleteBucketAsync(BucketName).Wait();
		}

		private static MongoDbEventStore<object> CreateTarget()
		{
			var cns = ConfigurationManager.ConnectionStrings["mongoTest"].ConnectionString;

			return new MongoDbEventStore<object>(cns);
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