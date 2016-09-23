
namespace NEStore
{
	public class CommitData<T> : CommitInfo
	{
		public T[] Events { get; set; }
	}
}