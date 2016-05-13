using System;

namespace StreamLedger
{
	public class CommitData
	{
		public long BucketRevision { get; set; }
		public Guid StreamId { get; set; }
		public int StreamRevisionStart { get; set; }
		public int StreamRevisionEnd { get; set; }
		public object[] Events { get; set; }
		public bool Dispatched { get; set; }
	}
}