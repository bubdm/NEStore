using System;

namespace NEStore
{
	public class CommitInfo
	{
		/// <summary>
	 /// Gets the value which identifies commit
	 /// </summary>
		public long BucketRevision { get; set; }

		/// <summary>
		/// Gets the value which identifies stream
		/// </summary>
		public Guid StreamId { get; set; }

		/// <summary>
		/// Gets the value which identifies the stream start revision of the commit
		/// </summary>
		public int StreamRevisionStart { get; set; }

		/// <summary>
		/// Gets the value which identifies the stream end revision of the commit
		/// </summary>
		public int StreamRevisionEnd { get; set; }

		/// <summary>
		/// Gets the value which identifies the correct dispatch of events belonging to the commit
		/// </summary>
		public bool Dispatched { get; set; }
	}
}