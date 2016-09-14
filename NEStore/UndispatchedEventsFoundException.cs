using System;
using System.Runtime.Serialization;

namespace NEStore
{
	[Serializable]
	public class UndispatchedEventsFoundException : Exception
	{
		//
		// For guidelines regarding the creation of new exception types, see
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
		// and
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
		//

		public UndispatchedEventsFoundException()
		{
		}

		public UndispatchedEventsFoundException(string message) : base(message)
		{
		}

		public UndispatchedEventsFoundException(string message, Exception inner) : base(message, inner)
		{
		}

		protected UndispatchedEventsFoundException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}
