using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace NEStore.MongoDb
{
	public static class MongoExceptionExtension
	{
		public static bool IsDuplicateKeyException(this MongoWriteException ex)
		{
			return ex.WriteError != null && ex.WriteError.Code == 11000;
		}
	}
}
