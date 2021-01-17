using System.Collections.Concurrent;
using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using ShardEqualizer.LocalStoring;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer
{
	public class CollStatsLocalStore
	{
		private readonly LocalStore<CollStatsContainer> _store;

		private readonly ConcurrentDictionary<CollectionNamespace, CollectionStatistics> _map =
			new ConcurrentDictionary<CollectionNamespace, CollectionStatistics>();

		public CollStatsLocalStore(LocalStoreProvider storeProvider)
		{
			_store = storeProvider.Create<CollStatsContainer>("collStats", onSave);

			if (_store.Container.Stats != null)
				_map = new ConcurrentDictionary<CollectionNamespace, CollectionStatistics>(_store.Container.Stats);
		}

		private void onSave(CollStatsContainer container)
		{
			container.Stats = new Dictionary<CollectionNamespace, CollectionStatistics>(_map);
		}

		public CollectionStatistics FindCollStats(CollectionNamespace ns)
		{
			return _map.TryGetValue(ns, out var result)
				? result
				: null;
		}

		public void SaveCollStats(CollectionNamespace ns, CollectionStatistics collStats)
		{
			_map[ns] = collStats;
			_store.OnChanged();
		}

		private class CollStatsContainer: Container
		{
			[BsonElement("collStats"), BsonDictionaryOptions(DictionaryRepresentation.Document), BsonIgnoreIfNull]
			public Dictionary<CollectionNamespace, CollectionStatistics> Stats { get; set; }
		}
	}
}
