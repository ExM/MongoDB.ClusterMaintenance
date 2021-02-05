using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;
using ShardEqualizer.LocalStoring;
using ShardEqualizer.Models;

namespace ShardEqualizer
{
	public class ShardListService
	{
		private readonly ShardRepository _repo;
		private readonly ProgressRenderer _progressRenderer;
		private readonly LocalStore<AllShardsContainer> _store;

		public ShardListService(
			ShardRepository repo,
			ProgressRenderer progressRenderer,
			LocalStoreProvider storeProvider)
		{
			_repo = repo;
			_progressRenderer = progressRenderer;
			_store = storeProvider.Create<AllShardsContainer>("shards");
		}

		public async Task<IReadOnlyCollection<Shard>> Get(CancellationToken token)
		{
			if (_store.Container.Shards == null)
			{
				await using var reporter = _progressRenderer.Start("Load shard list");
				var result = await _repo.GetAll(token);
				reporter.SetCompleteMessage($"found {result.Count} shards.");
				_store.Container.Shards = result;
				_store.OnChanged();
			}

			return _store.Container.Shards;
		}

		private class AllShardsContainer: Container
		{
			[BsonElement("shards"), BsonRequired]
			public IReadOnlyCollection<Shard> Shards { get; set; }
		}
	}
}
