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

		public static void Register(Type type, string discriminatorName)
		{
			if (BsonClassMap.IsClassMapRegistered(type))
				return;

			var cm = new BsonClassMap(type);
			cm.AutoMap();
			cm.SetDiscriminatorIsRequired(true);
			cm.SetDiscriminator(discriminatorName);

			BsonClassMap.RegisterClassMap(cm);
		}
	}
}
