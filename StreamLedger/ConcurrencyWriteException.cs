using System;
using System.Runtime.Serialization;

namespace StreamLedger
{
	[Serializable]
	public class ConcurrencyWriteException : Exception
	{
		//
		// For guidelines regarding the creation of new exception types, see
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
		// and
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
		//

		public ConcurrencyWriteException()
		{
		}

		public ConcurrencyWriteException(string message) : base(message)
		{
		}

		public ConcurrencyWriteException(string message, Exception inner) : base(message, inner)
		{
		}

		protected ConcurrencyWriteException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}
