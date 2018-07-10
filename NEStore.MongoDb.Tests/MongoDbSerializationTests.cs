using System;
using System.Collections.Generic;
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
	}
}