using System;
using System.Linq;
using MongoDB.Bson.Serialization;

namespace NEStore.MongoDb
{
	public static class MongoDbSerialization
	{
		static MongoDbSerialization()
		{
			// This is the same for all instances, I need to register only once
			BsonClassMap.RegisterClassMap<CommitInfo>(cm =>
			{
				cm.MapIdProperty(c => c.BucketRevision);
				cm.AutoMap();
				cm.SetIgnoreExtraElements(true);
			});
		}

		internal static void RegisterCommitData<T>()
		{
			BsonClassMap.RegisterClassMap<CommitData<T>>(cm =>
			{
				cm.AutoMap();
				cm.SetIgnoreExtraElements(true);
			});
		}

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
