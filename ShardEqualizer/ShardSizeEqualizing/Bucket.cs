using MongoDB.Driver;
using ShardEqualizer.Models;

namespace ShardEqualizer.ShardSizeEqualizing
{
	public class Bucket
	{
		private long _minSize;

		public Bucket(CollectionNamespace collection, ShardIdentity shard)
		{
			Shard = shard;
			Collection = collection;
			CurrentSize = 0;
			Managed = false;
			_minSize = 0;
		}

		public ShardIdentity Shard { get; }
		public CollectionNamespace Collection { get; }
		public long CurrentSize { get; set; }
		public bool Managed { get; set; }

		public long MinSize
		{
			get => _minSize;
			set => _minSize = value < 0 ? 0 : value;
		}
	}
}