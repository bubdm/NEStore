using System;
using System.Configuration;
using System.Linq;

namespace StreamLedger.MongoDb.UnitTests
{
	public class BucketManagerFixture : IDisposable
	{
		public string BucketName { get; }
		public MongoDbBucketManager Target { get; }

		public BucketManagerFixture()
		{
			BucketName = RandomString(10);
			Target = CreateTarget();
		}

		public void Dispose()
		{
			Target.DeleteBucketAsync(BucketName).Wait();
		}

		public MongoDbBucketManager CreateTarget()
		{
			var cns = ConfigurationManager.ConnectionStrings["mongoTest"].ConnectionString;

			return new MongoDbBucketManager(cns);
		}

		public static string RandomString(int length)
		{
			const string chars = "abcdefghijklmnopqrstuvwxyz";
			var random = new Random();
			return new string(Enumerable.Repeat(chars, length)
				.Select(s => s[random.Next(s.Length)]).ToArray());
		}

	}
}