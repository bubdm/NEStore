using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamLedger.MongoDb
{
	public class CommitContract
	{
		public long BucketRevision { get; set; }

		public Guid StreamId { get; set; }

		public int FromStreamRevision { get; set; }
		public int ToStreamRevision { get; set; }
	}
}
