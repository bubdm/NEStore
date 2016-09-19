using System.Threading.Tasks;

namespace NEStore
{
	public class WriteResult<T>
	{
		public WriteResult(CommitData<T> commit, Task dispatchTask)
		{
			DispatchTask = dispatchTask;
			Commit = commit;
		}

		public CommitData<T> Commit { get; private set; }
		public Task DispatchTask { get; private set; }
	}
}