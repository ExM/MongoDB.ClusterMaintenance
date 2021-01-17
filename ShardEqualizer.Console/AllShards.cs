using System.Collections.Generic;
using MongoDB.Driver;
using ShardEqualizer.Models;

namespace ShardEqualizer
{
	public class AllShards : List<Shard>
	{
		public AllShards(IEnumerable<Shard> items) : base(items)
		{
		}
	}
}
