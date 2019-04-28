using System;
using System.Text;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using NUnit.Framework;

namespace MongoDB.ClusterMaintenance
{
	[TestFixture]
	public class BsonSplitterTests
	{
		[SetUp]
		public void Setup()
		{
		}

		[Test]
		public void DemoGuid()
		{
			var bounds = BsonSplitter.Split((BsonValue) Guid.Empty, (BsonValue) Guid.Parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"), 13);

			foreach (var bound in bounds)
			{
				var hex = ByteArrayToString(((BsonBinaryData) bound).Bytes);
				
				var jsonSettings = new JsonWriterSettings() {GuidRepresentation = GuidRepresentation.CSharpLegacy};
				
				Console.WriteLine("{0} {1}", hex, bound.ToJson(jsonSettings));
			}
		}
		
		[Test]
		public void DemoObjectId()
		{
			var bounds = BsonSplitter.Split(ObjectId.Parse("800000000000000000000000"), ObjectId.Parse("ffffffffffffffffffffffff"), 17);

			foreach (var bound in bounds)
			{
				var hex = ByteArrayToString(((ObjectId) bound).ToByteArray());
				
				var jsonSettings = new JsonWriterSettings() {GuidRepresentation = GuidRepresentation.CSharpLegacy};
				
				Console.WriteLine("{0} {1}", hex, bound.ToJson(jsonSettings));
			}
		}
		
		public static string ByteArrayToString(byte[] ba)
		{
			StringBuilder hex = new StringBuilder(ba.Length * 2);
			foreach (byte b in ba)
				hex.AppendFormat("{0:x2}", b);
			return hex.ToString();
		}
	}
}