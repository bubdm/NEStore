using System;
using System.Threading.Tasks;

namespace NEStore.MongoDb.UndispatchedStrategies
{
	/// <summary>
	/// This strategy dispatch all the commits undispatched, only if not dispached after AutoDispatchWaitTime.
	/// If there are always undispatched and MaxWaitTime is passed, then UndispatchedEventsFoundException is throw.
	/// NOTE: This is a best effort strategy, can always happen that some commit will be inserted after that check...
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class UndispatchAllStrategy<T> : IUndispatchedStrategy<T>
	{
		/// <summary>
		/// When undispatched events are found, wait for this interval and redispatch if required. Default is 10s.
		/// </summary>
		public TimeSpan AutoDispatchWaitTime { get; set; } = TimeSpan.FromSeconds(10);

		public TimeSpan AutoDispatchCheckInterval { get; set; } = TimeSpan.FromMilliseconds(100);

		public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromMinutes(1);

		public async Task CheckUndispatchedAsync(IBucket<T> bucket, Guid streamId)
		{
			var sameCommitWait = TimeSpan.Zero;
			var totalWait = TimeSpan.Zero;

			var prevUndispachedRevision = long.MinValue;
			while (true)
			{
				// Check if there is an undispatched
				var undispatched = await bucket.GetFirstUndispatchedCommitAsync()
					.ConfigureAwait(false);

				// No more undispatched
				if (undispatched == null)
					break;

				// Is the same commit as before? if not reset the wait time
				if (prevUndispachedRevision != undispatched.BucketRevision)
				{
					sameCommitWait = TimeSpan.Zero;
					prevUndispachedRevision = undispatched.BucketRevision;
				}

				// Wait
				await Task.Delay(AutoDispatchCheckInterval)
					.ConfigureAwait(false);

				totalWait += AutoDispatchCheckInterval;
				sameCommitWait += AutoDispatchCheckInterval;

				if (totalWait >= MaxWaitTime)
					throw new UndispatchedEventsFoundException("Undispatched events found");

				if (sameCommitWait >= AutoDispatchWaitTime)
					await DispatchLastCommitAsync(bucket, undispatched.BucketRevision)
						.ConfigureAwait(false);
			}
		}

		private static async Task DispatchLastCommitAsync(IBucket<T> bucket, long atBucketRevision)
		{
			try
			{
				await bucket.DispatchUndispatchedAsync(null, atBucketRevision)
					.ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				throw new UndispatchedEventsFoundException("Undispatched events found, cannot dispatch them and write new events", ex);
			}
		}
	}
}
