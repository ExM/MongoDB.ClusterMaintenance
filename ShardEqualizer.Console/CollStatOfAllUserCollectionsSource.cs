using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using ShardEqualizer.MongoCommands;

namespace ShardEqualizer
{
	public class CollStatOfAllUserCollectionsSource : IDataSource<CollStatOfAllUserCollections>
	{
		private readonly ProgressRenderer _progressRenderer;
		private readonly CollectionStatisticService _collStatsService;
		private readonly IDataSource<UserCollections> _sourceUserCollections;

		public CollStatOfAllUserCollectionsSource(ProgressRenderer progressRenderer, CollectionStatisticService collStatsService, IDataSource<UserCollections> sourceUserCollections)
		{
			_progressRenderer = progressRenderer;
			_collStatsService = collStatsService;
			_sourceUserCollections = sourceUserCollections;
		}

		public async Task<CollStatOfAllUserCollections> Get(CancellationToken token)
		{
			var userCollections = await _sourceUserCollections.Get(token);

			await using var reporter = _progressRenderer.Start("Load collection statistics", userCollections.Count);

			async Task<CollStatsResult> runCollStats(CollectionNamespace ns, CancellationToken t)
			{
				var collStats = await _collStatsService.Get(ns, t);
				reporter.Increment();
				return collStats;
			}

			var allCollStats = await userCollections.ParallelsAsync(runCollStats, 32, token);

			return new CollStatOfAllUserCollections(allCollStats.ToDictionary(_ => _.Ns));
		}
	}
}
