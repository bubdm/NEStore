using System;
using System.Linq;
using MongoDB.Bson.Serialization;

namespace NEStore.MongoDb
{
	public static class MongoDbSerialization
	{
		public static void Register(Type type)
		{
			Register(type, GetDiscriminatorName(type));
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

		public static string GetDiscriminatorName(Type type)
		{
			if (!type.IsGenericType)
				return type.Name;

			string genericTypeName = type.GetGenericTypeDefinition().Name;
			genericTypeName = genericTypeName.Substring(0, genericTypeName.IndexOf('`'));

			string genericArgs = string.Join(",", type.GetGenericArguments().Select(GetDiscriminatorName));
			return genericTypeName + "<" + genericArgs + ">";
		}
	}
}
