using System;
using System.Configuration;
using System.Linq;
using Moq;

namespace StreamLedger.MongoDb.Tests
{
	public class MongoDbLedgerFixture : IDisposable
	{
		public string BucketName { get; }
		public MongoDbLedger Target { get; }
		public MongoDbBucket Bucket { get; }
		public Mock<IEventDispatcher> Dispatcher { get; }

		public MongoDbLedgerFixture()
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

		private static MongoDbLedger CreateTarget()
		{
			var cns = ConfigurationManager.ConnectionStrings["mongoTest"].ConnectionString;

			return new MongoDbLedger(cns);
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