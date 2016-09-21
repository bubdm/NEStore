using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.Options;

namespace NEStore.MongoDb.Conventions
{
	/// <summary>
	/// A convention that allow to specify a different dictionary representation for non string key dictionaries
	/// </summary>
	public class SafeDictionaryKeyConvention : ConventionBase, IMemberMapConvention
	{
		private readonly DictionaryRepresentation _dictionaryRepresentation;
		public SafeDictionaryKeyConvention(DictionaryRepresentation dictionaryRepresentation = DictionaryRepresentation.ArrayOfDocuments)
		{
			_dictionaryRepresentation = dictionaryRepresentation;
		}

		public void Apply(BsonMemberMap memberMap)
		{
			var type = memberMap.MemberType;

			if (type.IsGenericType &&
			    (type.GetGenericTypeDefinition() == typeof(Dictionary<,>) ||
					type.GetGenericTypeDefinition() == typeof(IDictionary<,>)) &&
			    type.GetGenericArguments().FirstOrDefault() != typeof(string))
			{
				memberMap.SetSerializer(ConfigureSerializer(memberMap.GetSerializer()));
			}
		}

		private IBsonSerializer ConfigureSerializer(IBsonSerializer serializer)
		{
			var dictionaryRepresentationConfigurable = serializer as IDictionaryRepresentationConfigurable;
			if (dictionaryRepresentationConfigurable != null)
			{
				serializer = dictionaryRepresentationConfigurable.WithDictionaryRepresentation(_dictionaryRepresentation);
			}

			var childSerializerConfigurable = serializer as IChildSerializerConfigurable;
			return childSerializerConfigurable == null
					? serializer
					: childSerializerConfigurable.WithChildSerializer(ConfigureSerializer(childSerializerConfigurable.ChildSerializer));
		}
	}
}
