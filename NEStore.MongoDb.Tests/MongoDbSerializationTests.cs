//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;
//using MongoDB.Bson;
//using MongoDB.Bson.IO;
//using MongoDB.Bson.Serialization;
//using MongoDB.Bson.Serialization.Conventions;
//using MongoDB.Bson.Serialization.Serializers;
//using Xunit;

//namespace NEStore.MongoDb.Tests
//{
//	public class MongoDbSerializationTests
//	{
//		[Fact]
//		public async Task Deserialize_Simple_Types()
//		{
//			BsonSerializer.RegisterDiscriminatorConvention(typeof(object), new TestDiscriminatorConvention("_t"));

//			using (var fixture = new MongoDbEventStoreFixture())
//			{
//				var streamId = Guid.NewGuid();
//				var json =
//					"{ '_id' : NumberLong(1), 'StreamRevisionStart' : 0, 'StreamRevisionEnd' : 1, 'Events' : [ { '_t' : 'SimpleEvent', 'Foo' : 'f1' } ], 'Dispatched' : true}";

//				var bson = BsonDocument.Parse(json);
//				bson["StreamId"] = BsonValue.Create(streamId);

//				await fixture.EventStore
//					.Database
//					.GetCollection<BsonDocument>($"{fixture.BucketName}.commits")
//					.InsertOneAsync(bson);

//				var eventsRead = await fixture.Bucket.GetEventsAsync(streamId);

//				Assert.Equal(1, eventsRead.Count());

//				eventsRead = await fixture.Bucket.GetEventsAsync(streamId);
//			}
//		}

//		[Fact]
//		public async Task Serialize_Simple_Types()
//		{
//			using (var fixture = new MongoDbEventStoreFixture())
//			{
//				//MongoDbSerialization.Register(typeof(GenericEvent<GenericType,GenericType>));

//				var streamId = Guid.NewGuid();
//				var events = new List<SimpleEvent>
//				{
//					new SimpleEvent() {Foo = "f1"}
//				};

//				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, events);
//				var eventsRead = await fixture.Bucket.GetEventsAsync(streamId);

//				Assert.Equal(events.Count, eventsRead.Count());
//			}
//		}

//		[Fact]
//		public async Task Serialize_Generics_Types()
//		{
//			using (var fixture = new MongoDbEventStoreFixture())
//			{
//				//MongoDbSerialization.Register(typeof(GenericEvent<GenericType,GenericType>));

//				var streamId = Guid.NewGuid();
//				var events = new List<GenericEvent<GenericType,GenericType>>();
				
//				events.Add(new GenericEvent<GenericType, GenericType>(
//					new GenericType() {Foo = "f1"}, new GenericType() { Foo = "f2"}
//					));

//				events.Add(new GenericEvent<GenericType, GenericType>(
//					new GenericType() { Foo = "f1" }, new GenericType2() { Foo = "f2", Boo = "b1" }
//					));

//				await fixture.Bucket.WriteAndDispatchAsync(streamId, 0, events);
//				var eventsRead = await fixture.Bucket.GetEventsAsync(streamId);

//				Assert.Equal(events.Count, eventsRead.Count());
//			}
//		}

//		[Fact]
//		public async Task Deserialize_Generic_Types()
//		{
//			//BsonSerializer.RegisterDiscriminatorConvention(typeof(object), new TestDiscriminatorConvention("_t"));
//			MongoDbSerialization.Register(typeof(GenericEvent<GenericType, GenericType>), "G1");
//			MongoDbSerialization.Register(typeof(GenericEvent<GenericType, GenericType2>), "G2");
//			MongoDbSerialization.Register(typeof(GenericEvent<GenericType2, GenericType>), "G3");
//			MongoDbSerialization.Register(typeof(GenericEvent<GenericType2, GenericType2>), "G4");

//			using (var fixture = new MongoDbEventStoreFixture())
//			{
//				var streamId = Guid.NewGuid();
//				var json =
//					"{ '_id' : NumberLong(1), 'StreamRevisionStart' : 0, 'StreamRevisionEnd' : 2, 'Events' : [ { '_t' : 'G1', 'Uprop' : { 'Foo' : 'f2' }, 'Tprop' : { 'Foo' : 'f1' } }, { '_t' : 'G2', 'Uprop' : { '_t' : 'GenericType2', 'Foo' : 'f2', 'Boo' : 'b1' }, 'Tprop' : { 'Foo' : 'f1' } } ], 'Dispatched' : true}";

//				var bson = BsonDocument.Parse(json);
//				bson["StreamId"] = BsonValue.Create(streamId);

//				await fixture.EventStore
//					.Database
//					.GetCollection<BsonDocument>($"{fixture.BucketName}.commits")
//					.InsertOneAsync(bson);

//				var eventsRead = await fixture.Bucket.GetEventsAsync(streamId);

//				Assert.Equal(2, eventsRead.Count());
//			}
//		}
//	}


//	public class SimpleEvent
//	{
//		public string Foo { get; set; }

//	}

//	public class GenericEvent <T,U>
//		where T : GenericType
//		where U : GenericType
//	{
//		public U Uprop { get; set; }
//		public T Tprop { get; set; }

//		public GenericEvent(T tprop, U uprop)
//		{
//			Tprop = tprop;
//			Uprop = uprop;
//		}
//	}

//	public class GenericType
//	{
//		public string Foo { get; set; }
//	}

//	public class GenericType2 : GenericType
//	{
//		public string Boo { get; set; }
//	}

//	public class TestDiscriminatorConvention : IDiscriminatorConvention
//	{
//		private readonly HierarchicalDiscriminatorConvention _standardConvention;
//		public TestDiscriminatorConvention(string elementName)
//		{
//			_standardConvention = new HierarchicalDiscriminatorConvention(elementName);
//		}

//		public Type GetActualType(IBsonReader bsonReader, Type nominalType)
//		{
//			var bookmark = bsonReader.GetBookmark();
//			try
//			{
//				return _standardConvention.GetActualType(bsonReader, nominalType);
//			}
//			catch (BsonSerializationException)
//			{
//				bsonReader.ReturnToBookmark(bookmark);
//				var discriminatorValue = GetDiscriminatorValue(bsonReader, nominalType);
//				switch (discriminatorValue)
//				{
//					case "SimpleEvent":
//						MongoDbSerialization.Register(typeof(SimpleEvent));
//						return typeof(SimpleEvent);
//					case "GenericEvent`2":
//						MongoDbSerialization.Register(typeof(GenericEvent<GenericType, GenericType>));
//						return typeof(GenericEvent<GenericType, GenericType>);
//				}

//				throw;
//			}
//		}

//		private string GetDiscriminatorValue(IBsonReader bsonReader, Type nominalType)
//		{
//			string discriminatorValue = null;

//			var bsonType = bsonReader.GetCurrentBsonType();
//			if (bsonType == BsonType.Document)
//			{
//				if (BsonSerializer.IsTypeDiscriminated(nominalType))
//				{
//					var bookmark = bsonReader.GetBookmark();
//					bsonReader.ReadStartDocument();
//					if (bsonReader.FindElement(ElementName))
//					{
//						var context = BsonDeserializationContext.CreateRoot(bsonReader);
//						var discriminator = BsonValueSerializer.Instance.Deserialize(context);
//						if (discriminator.IsBsonArray)
//						{
//							discriminator = discriminator.AsBsonArray.Last(); // last item is leaf class discriminator
//						}

//						discriminatorValue = discriminator.AsString;
//					}

//					bsonReader.ReturnToBookmark(bookmark);
//				}
//			}

//			return discriminatorValue;
//		}

//		public BsonValue GetDiscriminator(Type nominalType, Type actualType)
//		{
//			return _standardConvention.GetDiscriminator(nominalType, actualType);
//		}

//		public string ElementName => _standardConvention.ElementName;
//	}
//}
