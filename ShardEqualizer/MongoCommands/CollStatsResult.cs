using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using ShardEqualizer.Models;

namespace ShardEqualizer.MongoCommands
{
	public class CollStatsResult: CollStats
	{
		[BsonElement("primary"), BsonIgnoreIfNull]
		public ShardIdentity? Primary;

		[BsonElement("nchunks"), BsonIgnoreIfNull]
		public long? NChunks;

		[BsonElement("sharded"), BsonRequired]
		public bool Sharded;

		[BsonElement("shards"), BsonDictionaryOptions(DictionaryRepresentation.Document), BsonIgnoreIfNull]
		public IReadOnlyDictionary<ShardIdentity, CollStats> Shards;

		//[BsonExtraElements]
		//public BsonDocument ExtraElements;
	}
}
