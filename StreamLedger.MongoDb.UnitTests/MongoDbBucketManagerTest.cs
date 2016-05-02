using System;
using System.Configuration;
using Xunit;

namespace StreamLedger.MongoDb.UnitTests
{
	public class MongoDbBucketManagerTest
	{
		[Fact]
		public void Valid_Bucket_Name()
		{
			var target = CreateTarget();

			var invalidBuckets = new[]
			{
				null,
				"",
				"test test",
				"Vsm",
				"test(",
				"5test"
			};

			foreach (var bucket in invalidBuckets)
				Assert.ThrowsAsync<ArgumentException>(() => target.EnsureBucketAsync(bucket)).Wait();
		}

		private IBucketManager CreateTarget()
		{
			var cns = ConfigurationManager.ConnectionStrings["mongoTest"].ConnectionString;

			return new MongoDbBucketManager(cns);
		}
	}
}