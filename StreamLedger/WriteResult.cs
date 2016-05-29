using System.Threading.Tasks;

namespace StreamLedger
{
	public class WriteResult
	{
		public WriteResult(CommitData commit, Task dispatchTask)
		{
			DispatchTask = dispatchTask;
			Commit = commit;
		}

		public CommitData Commit { get; private set; }
		public Task DispatchTask { get; private set; }
	}
}