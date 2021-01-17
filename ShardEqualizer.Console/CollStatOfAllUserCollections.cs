using System.Collections.Generic;
using MongoDB.Driver;
using ShardEqualizer.Models;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer
{
	public class CollStatOfAllUserCollections : Dictionary<CollectionNamespace, CollectionStatistics>
	{
		public CollStatOfAllUserCollections(IDictionary<CollectionNamespace, CollectionStatistics> dictionary) : base(dictionary)
		{
		}

		public CollStatOfAllUserCollections(IEnumerable<KeyValuePair<CollectionNamespace, CollectionStatistics>> items) : base(items)
		{
		}
	}
}
