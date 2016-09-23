using System;
using System.Linq;
using MongoDB.Bson.Serialization;

namespace NEStore.MongoDb
{
	public static class MongoDbSerialization
	{
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

			string genericTypeName = type.GetGenericTypeDefinition().Name;
			genericTypeName = genericTypeName.Substring(0, genericTypeName.IndexOf('`'));

			string genericArgs = string.Join(",", type.GetGenericArguments().Select(GetDiscriminatorName));
			return genericTypeName + "<" + genericArgs + ">";
		}
	}
}
