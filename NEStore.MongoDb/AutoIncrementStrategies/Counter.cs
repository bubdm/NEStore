namespace NEStore.MongoDb.AutoIncrementStrategies
{
	internal class Counter
	{
		public string BucketName { get; set; }
		public long BucketRevision { get; set; }
	}
}