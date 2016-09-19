using System;

namespace NEStore
{
	public class CommitData<T>
	{
		public long BucketRevision { get; set; }
		public Guid StreamId { get; set; }
		public int StreamRevisionStart { get; set; }
		public int StreamRevisionEnd { get; set; }
		public T[] Events { get; set; }
		public bool Dispatched { get; set; }
	}
}