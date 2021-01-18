using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using ShardEqualizer.LocalStoring;
using ShardEqualizer.Models;

namespace ShardEqualizer
{
	public class ShardedCollectionService
	{
		private readonly CollectionRepository _repo;
		private readonly ProgressRenderer _progressRenderer;
		private readonly LocalStore<ShardedCollectionContainer> _store;

		private IReadOnlyDictionary<CollectionNamespace, ShardedCollectionInfo> _map;

		public ShardedCollectionService(
			CollectionRepository repo,
			ProgressRenderer progressRenderer,
			LocalStoreProvider storeProvider)
		{
			_repo = repo;
			_progressRenderer = progressRenderer;

			_store = storeProvider.Create<ShardedCollectionContainer>("shardedCollections", onSave);

			if (_store.Container.ShardedCollection != null)
				_map = _store.Container.ShardedCollection;
		}

		private void onSave(ShardedCollectionContainer container)
		{
			container.ShardedCollection = new Dictionary<CollectionNamespace, ShardedCollectionInfo>(_map);
		}

		public async Task<IReadOnlyDictionary<CollectionNamespace, ShardedCollectionInfo>> Get(CancellationToken token)
		{
			if (_map == null)
			{
				await using var reporter = _progressRenderer.Start("Load sharded collections");
				var result = await _repo.FindAll(false, token); //TODO maybe skip dropped items
				reporter.SetCompleteMessage($"found {result.Count} collections.");

				_map = result.ToDictionary(_ => _.Id);
				_store.OnChanged();
			}

			return _map;
		}

		private class ShardedCollectionContainer: Container
		{
			[BsonElement("shardedCollections"), BsonDictionaryOptions(DictionaryRepresentation.Document), BsonIgnoreIfNull]
			public Dictionary<CollectionNamespace, ShardedCollectionInfo> ShardedCollection { get; set; }
		}
	}
}
