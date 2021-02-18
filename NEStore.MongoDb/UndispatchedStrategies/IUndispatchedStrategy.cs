using System;
using System.Threading;
using System.Threading.Tasks;

namespace NEStore.MongoDb.UndispatchedStrategies
{
	/// <summary>
	/// When undispatched events are found, wait and double-check for undispatched status. Default is 1s.
	/// NOTE: Events are supposed to be idempotent, so they can be eventualy redispatched multiple times,
	///  but I want to ensure that this not happen in a short period of time.
	/// The same event can be redispatched in case of a temporary network problem, db problem, ...
	///  but normally the system ensure that an event is dispatched only once, also on heavy load scenario (concurrency)
	/// The idea is to ensure this by waiting (AutoDispatchWaitTime) and check if the system is busy doing dispatching.
	/// If the system doesn't dispatch any new events after the AutoDispatchWaitTime then it is "safe" to try to redispatch it
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public interface IUndispatchedStrategy<T>
	{
		/// <summary>
		/// Check if bucket has undispatched commits, if founded tries to dispatch them based on the strategy
		/// </summary>
		/// <returns>Throw UndispatchedEventsFoundException if undispatched commits exists and the process should exit</returns>
		Task CheckUndispatchedAsync(IBucket<T> bucket, Guid streamId, CancellationToken token = default);
	}
}
