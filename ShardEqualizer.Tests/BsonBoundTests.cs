using System;
using System.Linq;
using MongoDB.Bson;
using NUnit.Framework;
using ShardEqualizer.Models;

namespace ShardEqualizer
{
	[TestFixture]
	public class BsonBoundTests
	{
		[Test]
		public void ParseElementNames()
		{
			var bound = BsonBound.Parse("{ \"_id\" : NumberInt(10), \"n\": \"text\" }");

			var element = ((BsonDocument) bound).Elements.Select(_ => _.Name).ToList();

			CollectionAssert.AreEqual(new [] {"_id", "n"}, element);
		}

		[TestCase("00000000-0000-0000-0000-000000000000")]
		[TestCase("22345200-abe8-4f60-90c8-0d43c5f6c0f6")]
		[TestCase("ffffffff-ffff-ffff-ffff-ffffffffffff")]
		public void ParseGuid(string text)
		{
			var bound = BsonBound.Parse($"{{ \"_id\" : CSUUID(\"{text}\") }}");

			var element = ((BsonDocument) bound).Elements.Single();

			Assert.AreEqual(Guid.Parse(text), element.Value.AsGuid);
		}

		[Test]
		public void ParseMinKey()
		{
			var bound = BsonBound.Parse($"{{ \"_id\" : MinKey }}");

			var element = ((BsonDocument) bound).Elements.Single();

			Assert.IsTrue(element.Value.IsBsonMinKey);
		}

		[Test]
		public void ParseMaxKey()
		{
			var bound = BsonBound.Parse($"{{ \"_id\" : MaxKey }}");

			var element = ((BsonDocument) bound).Elements.Single();

			Assert.IsTrue(element.Value.IsBsonMaxKey);
		}

		[TestCase("000000000000000000000000")]
		[TestCase("800000000000000000000000")]
		[TestCase("ffffffffffffffffffffffff")]
		public void ParseObjectId(string text)
		{
			var bound = BsonBound.Parse($"{{ \"_id\" : ObjectId(\"{text}\") }}");

			var element = ((BsonDocument) bound).Elements.Single();

			Assert.AreEqual(ObjectId.Parse(text), element.Value.AsObjectId);
		}

		[Test]
		public void ParseInt()
		{
			var bound = BsonBound.Parse($"{{ \"_id\" : NumberInt(10) }}");

			var element = ((BsonDocument) bound).Elements.Single();

			Assert.AreEqual(10, element.Value.AsInt32);
		}
	}
}
