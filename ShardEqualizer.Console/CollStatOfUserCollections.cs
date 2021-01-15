using System.Collections.Generic;
using MongoDB.Driver;
using ShardEqualizer.MongoCommands;

namespace ShardEqualizer
{
	public class CollStatOfUserCollections : Dictionary<CollectionNamespace, CollStatsResult>
	{
		public CollStatOfUserCollections(IDictionary<CollectionNamespace, CollStatsResult> dictionary) : base(dictionary)
		{
		}
	}
}