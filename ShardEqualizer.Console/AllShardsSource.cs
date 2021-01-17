using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace ShardEqualizer
{
	public class AllShardsSource : IDataSource<AllShards>
	{
		private readonly ShardRepository _shardRepository;
		private readonly AllShardsLocalStore _localStore;
		private readonly ProgressRenderer _progressRenderer;

		public AllShardsSource(ShardRepository shardRepository, AllShardsLocalStore localStore, ProgressRenderer progressRenderer)
		{
			_shardRepository = shardRepository;
			_localStore = localStore;
			_progressRenderer = progressRenderer;
		}

		public async Task<AllShards> Get(CancellationToken token)
		{
			var result = _localStore.TryGet();
			if (result == null)
			{
				await using var reporter = _progressRenderer.Start("Load shard list");
				result = new AllShards(await _shardRepository.GetAll(token));
				reporter.SetCompleteMessage($"found {result.Count} shards.");

				_localStore.Save(result);
			}

			return result;
		}
	}
}
