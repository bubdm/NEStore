using System;
using System.Collections.Generic;
using MongoDB.Bson.Serialization;
using Xunit;

namespace NEStore.MongoDb.Tests
{
	public class MongoDbSerializationTests
	{
		[Theory]
		[InlineData("String", typeof(string))]
		[InlineData("Dictionary<String,Object>", typeof(Dictionary<string,object>))]
		[InlineData("List<Dictionary<String,Object>>", typeof(List<Dictionary<string, object>>))]
		[InlineData("Nullable<Int32>", typeof(int?))]
		public void GetDiscriminatorName(string exceptedName, Type type)
		{
			Assert.Equal(exceptedName, MongoDbSerialization.GetDiscriminatorName(type));
		}

		[Fact]
		public void Register()
		{
			// It should not fire an exception
			MongoDbSerialization.Register(typeof(DerivedClass));
			//var type = typeof(DerivedClass);

			//if (BsonClassMap.IsClassMapRegistered(type))
			//	return;

			//var cm = new BsonClassMap(type);
			//cm.AutoMap();

			//BsonClassMap.RegisterClassMap(cm);
		}

		//public abstract class UltraBaseClass
		//{
		//}

		public abstract class BaseClass //: UltraBaseClass
		{
			public Guid Id { get; protected set; }

			protected BaseClass(Guid id)
			{
				Id = id;
			}
		}

		public class DerivedClass : BaseClass
		{
			public DerivedClass(Guid id) : base(id)
			{
			}
		}
	}
}