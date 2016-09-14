using System;
using MongoDB.Bson.Serialization;

namespace NEStore.MongoDb
{
	public static class MongoDbSerialization
	{
		public static void Register(Type type)
		{
			BsonClassMap.LookupClassMap(type);
		}
	}
}
