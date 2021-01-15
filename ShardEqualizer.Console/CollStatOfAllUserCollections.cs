using System.Collections.Generic;
using MongoDB.Driver;
using ShardEqualizer.MongoCommands;

namespace ShardEqualizer
{
	public class CollStatOfAllUserCollections : Dictionary<CollectionNamespace, CollStatsResult>
	{
		public CollStatOfAllUserCollections(IDictionary<CollectionNamespace, CollStatsResult> dictionary) : base(dictionary)
		{
		}
	}
}
