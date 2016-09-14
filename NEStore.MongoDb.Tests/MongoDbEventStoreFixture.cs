using System;
using System.Configuration;
using System.Linq;
using Moq;

namespace NEStore.MongoDb.Tests
{
	public class MongoDbEventStoreFixture : IDisposable
	{
		public string BucketName { get; }
		public MongoDbEventStore Target { get; }
		public MongoDbBucket Bucket { get; }
		public Mock<IEventDispatcher> Dispatcher { get; }

		public MongoDbEventStoreFixture()
		{
			BucketName = RandomString(10);
			Target = CreateTarget();
			Dispatcher = new Mock<IEventDispatcher>();
			Target.RegisterDispatchers(Dispatcher.Object);
			Bucket = Target.Bucket(BucketName) as MongoDbBucket;
		}

		public void Dispose()
		{
			CleanUp();
		}

		public void CleanUp()
		{
			Target.DeleteBucketAsync(BucketName).Wait();
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