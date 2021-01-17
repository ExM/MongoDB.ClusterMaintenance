using System;
using MongoDB.Bson.Serialization.Attributes;

namespace ShardEqualizer.LocalStoring
{
	public abstract class Container
	{
		[BsonElement("date"), BsonRequired]
		public DateTime Date { get; set; }
	}
}