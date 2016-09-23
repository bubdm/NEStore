
namespace NEStore
{
	public class CommitData<T> : CommitInfo
	{
		
		/// /// <summary>
		/// Gets the list of committed events
		/// </summary>
		public T[] Events { get; set; }
	}
}