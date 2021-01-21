using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using ShardEqualizer.Models;

namespace ShardEqualizer.ShortModels
{
	public class ChunkInfo
	{
		public ChunkInfo()
		{
		}

		public ChunkInfo(Chunk chunk)
		{
			Min = chunk.Min;
			Max = chunk.Max;
			Shard = chunk.Shard;
			Jumbo = chunk.Jumbo;
		}

		[BsonElement("min"), BsonRequired]
		public BsonBound Min { get; set; }

		[BsonElement("max"), BsonRequired]
		public BsonBound Max { get; set; }

		[BsonElement("shard"), BsonRequired]
		public ShardIdentity Shard { get; set; }

		[BsonElement("jumbo"), BsonIgnoreIfDefault]
		public bool Jumbo { get; set; }
	}
}
