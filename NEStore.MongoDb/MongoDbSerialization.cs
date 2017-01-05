using System;
using System.Linq;
using MongoDB.Bson.Serialization;
using NEStore.MongoDb.AutoIncrementStrategies;

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

			BsonClassMap.RegisterClassMap<Counter>(cm =>
			{
				cm.MapIdProperty(c => c.BucketName);
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

		/// <summary>
		/// Register a type to be serialized correctly on MongoDB
		/// </summary>
		/// <param name="type">Object type</param>
		public static void Register(Type type)
		{
			Register(type, GetDiscriminatorName(type));
		}

		/// <summary>
		/// Register a type to be serialized correctly on MongoDB
		/// </summary>
		/// <param name="type">Object type</param>
		/// <param name="discriminatorName">Unique name that will be associated to provided type</param>
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

		/// <summary>
		/// Get a unique name for a Type
		/// </summary>
		/// <param name="type">Object type</param>
		/// <returns>Unique name, for generic Type the format will be Type &lt;T1,T2&gt;</returns>
		public static string GetDiscriminatorName(Type type)
		{
			if (!type.IsGenericType)
				return type.Name;

			var genericTypeName = type.GetGenericTypeDefinition().Name;
			genericTypeName = genericTypeName.Substring(0, genericTypeName.IndexOf('`'));

			var genericArgs = string.Join(",", type.GetGenericArguments().Select(GetDiscriminatorName));
			return genericTypeName + "<" + genericArgs + ">";
		}
	}
}
