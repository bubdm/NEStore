using System;
using System.Threading;
using System.Threading.Tasks;

namespace NEStore.MongoDb.UndispatchedStrategies
{
	// TODO Eval to use partial index for dispatched (only when dispatched is false)
	//  https://docs.mongodb.com/manual/core/index-partial/
	//  This will allow us to not check for dispatched when writing and just catch the duplicate exception
	//  and it is the db that will enforce to have just 1 undispatched per bucket

	/// <summary>
	/// This strategy dispatch all the commits undispatched, only if not dispached after AutoDispatchWaitTime.
	/// If there are always undispatched and MaxWaitTime is passed, then UndispatchedEventsFoundException is throw.
	/// NOTE: This is a best effort strategy, can always happen that some commit will be inserted after that check...
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class UndispatchAllStrategy<T> : IUndispatchedStrategy<T>
	{
		/// <summary>
		/// When undispatched events are found, wait for this interval and redispatch if required. Default is 15s.
		/// </summary>
		public TimeSpan AutoDispatchWaitTime { get; set; } = TimeSpan.FromSeconds(15);

		public TimeSpan AutoDispatchCheckInterval { get; set; } = TimeSpan.FromMilliseconds(100);

		public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromMinutes(1);

		/// <summary>
		/// Redispatch only for the same stream. Default is false.
		/// </summary>
		public bool SameStreamOnly { get; set; } = false;

		public async Task CheckUndispatchedAsync(IBucket<T> bucket, Guid streamId, CancellationToken token = default)
		{
			var sameCommitWait = TimeSpan.Zero;
			var totalWait = TimeSpan.Zero;
			var filterByStreamId = SameStreamOnly
				? (Guid?)streamId
				: null;

			var prevUndispachedRevision = long.MinValue;
			while (true)
			{
				// Check if there is an undispatched
				var undispatched = await bucket.GetFirstUndispatchedCommitAsync(filterByStreamId, token)
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
				await Task.Delay(AutoDispatchCheckInterval, token)
					.ConfigureAwait(false);

				totalWait += AutoDispatchCheckInterval;
				sameCommitWait += AutoDispatchCheckInterval;

				if (totalWait >= MaxWaitTime)
					throw new UndispatchedEventsFoundException("Undispatched events found");

				if (sameCommitWait >= AutoDispatchWaitTime)
					await DispatchLastCommitAsync(bucket, filterByStreamId, undispatched.BucketRevision, token)
						.ConfigureAwait(false);
			}
		}

		private static async Task DispatchLastCommitAsync(IBucket<T> bucket, Guid? filterByStreamId, long atBucketRevision, CancellationToken token = default)
		{
			try
			{
				await bucket.DispatchUndispatchedAsync(filterByStreamId, atBucketRevision, token)
					.ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				throw new UndispatchedEventsFoundException("Undispatched events found, cannot dispatch them and write new events", ex);
			}
		}
	}
}
