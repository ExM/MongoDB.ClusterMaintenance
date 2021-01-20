using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using ShardEqualizer.LocalStoring;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer
{
	public class CollectionStatisticService
	{
		private readonly IMongoClient _mongoClient;
		private readonly ProgressRenderer _progressRenderer;
		private readonly LocalStore<CollStatsContainer> _store;

		private readonly ConcurrentDictionary<CollectionNamespace, CollectionStatistics> _map = new ConcurrentDictionary<CollectionNamespace, CollectionStatistics>();

		public CollectionStatisticService(
			IMongoClient mongoClient,
			ProgressRenderer progressRenderer,
			LocalStoreProvider storeProvider)
		{
			_mongoClient = mongoClient;
			_progressRenderer = progressRenderer;

			_store = storeProvider.Create<CollStatsContainer>("collStats", onSave);

			if (_store.Container.Stats != null)
				foreach (var (key, value) in _store.Container.Stats)
					_map[key] = value;
		}

		private void onSave(CollStatsContainer container)
		{
			container.Stats = new Dictionary<CollectionNamespace, CollectionStatistics>(_map);
		}

		public async Task<IReadOnlyDictionary<CollectionNamespace, CollectionStatistics>> Get(IEnumerable<CollectionNamespace> nsList, CancellationToken token)
		{
			var nsSet = new HashSet<CollectionNamespace>(nsList);

			var missedKeys = nsSet.Except(_map.Keys).ToList();

			if (missedKeys.Any())
			{
				await using var reporter = _progressRenderer.Start($"Load collection statistics", missedKeys.Count);
				{
					async Task getCollStat(CollectionNamespace ns,
						CancellationToken t)
					{
						var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
						var collStat = await db.CollStats(ns.CollectionName, 1, t);
						reporter.Increment();
						_map[ns] = new CollectionStatistics(collStat);
					}

					await missedKeys.ParallelsAsync(getCollStat, 32, token);
				}
				_store.OnChanged();
			}

			return nsSet.ToDictionary(_ => _, _ => _map[_]);
		}

		private class CollStatsContainer: Container
		{
			[BsonElement("collStats"), BsonDictionaryOptions(DictionaryRepresentation.Document), BsonIgnoreIfNull]
			public Dictionary<CollectionNamespace, CollectionStatistics> Stats { get; set; }
		}
	}
}
