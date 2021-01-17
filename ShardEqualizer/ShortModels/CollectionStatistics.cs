using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.Models;

namespace ShardEqualizer.ShortModels
{
	public class CollectionStatistics: ShardCollectionStatistics
	{
		public CollectionStatistics(): base()
		{
		}

		public CollectionStatistics(CollStatsResult collStat): base(collStat)
		{
			Primary = collStat.Primary;
			Sharded = collStat.Sharded;

			if (collStat.Shards != null)
			{
				Shards = collStat.Shards
					.Select(_ =>
						new KeyValuePair<ShardIdentity, ShardCollectionStatistics>(_.Key,
							new ShardCollectionStatistics(_.Value))).ToDictionary(_ => _.Key, _ => _.Value);
			}
		}

		[BsonElement("primary"), BsonIgnoreIfNull]
		public ShardIdentity? Primary { get; set; }

		[BsonElement("sharded"), BsonRequired]
		public bool Sharded { get; set; }

		[BsonElement("shards"), BsonDictionaryOptions(DictionaryRepresentation.Document), BsonIgnoreIfNull]
		public IReadOnlyDictionary<ShardIdentity, ShardCollectionStatistics> Shards { get; set; }
	}
}
