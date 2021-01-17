using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using ShardEqualizer.LocalStoring;
using ShardEqualizer.Models;

namespace ShardEqualizer
{
	public class AllShardsLocalStore
	{
		private AllShards _shards;

		private readonly LocalStore<AllShardsContainer> _store;

		public AllShardsLocalStore(LocalStoreProvider storeProvider)
		{
			_store = storeProvider.Create<AllShardsContainer>("shards", onSave);

			if (_store.Container.Shards != null)
				_shards = new AllShards(_store.Container.Shards);
		}

		private void onSave(AllShardsContainer container)
		{
			container.Shards = _shards;
		}

		public AllShards TryGet()
		{
			return _shards;
		}

		public void Save(AllShards userCollections)
		{
			_shards = userCollections;
			_store.OnChanged();
		}

		private class AllShardsContainer: Container
		{
			[BsonElement("shards"), BsonRequired]
			public IList<Shard> Shards { get; set; }
		}
	}
}
