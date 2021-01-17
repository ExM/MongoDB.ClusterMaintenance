using MongoDB.Bson.Serialization.Attributes;
using ShardEqualizer.MongoCommands;

namespace ShardEqualizer.ShortModels
{
	public class ShardCollectionStatistics
	{
		public ShardCollectionStatistics()
		{
		}

		public ShardCollectionStatistics(CollStats collStats)
		{
			Size = collStats.Size;
			Count = collStats.Count;
			StorageSize = collStats.StorageSize;
			TotalIndexSize = collStats.TotalIndexSize;
		}

		[BsonElement("size"), BsonRequired]
		public long Size { get; set; }

		[BsonElement("count"), BsonRequired]
		public long Count { get; set; }

		[BsonElement("storageSize"), BsonRequired]
		public long StorageSize { get; set; }

		[BsonElement("totalIndexSize"), BsonRequired]
		public long TotalIndexSize { get; set; }
	}
}
