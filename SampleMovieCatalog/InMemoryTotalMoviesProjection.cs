using System.Threading.Tasks;
using StreamLedger.Aggregates;

namespace SampleMovieCatalog
{
	public class InMemoryTotalMoviesProjection : ProjectionBase
	{
		public int TotalMovies { get; set; }

		// ReSharper disable UnusedMember.Local
		// ReSharper disable UnusedParameter.Local
		private void On(Movie.Created @event) => TotalMovies++;
		// ReSharper restore UnusedParameter.Local
		// ReSharper restore UnusedMember.Local

		public override Task ClearAsync()
		{
			TotalMovies = 0;
			return Task.FromResult(false);
		}
	}
}
