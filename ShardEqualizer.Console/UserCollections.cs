using System.Collections.Generic;
using MongoDB.Driver;

namespace ShardEqualizer
{
	public class UserCollections : List<CollectionNamespace>
	{
		public UserCollections(IEnumerable<CollectionNamespace> items) : base(items)
		{
		}
	}
}