using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer
{
	public class CollStatOfAllUserCollectionsSource : IDataSource<CollStatOfAllUserCollections>
	{
		private readonly ProgressRenderer _progressRenderer;
		private readonly CollectionStatisticService _collStatsService;
		private readonly CollectionListService _sourceUserCollections;

		public CollStatOfAllUserCollectionsSource(ProgressRenderer progressRenderer, CollectionStatisticService collStatsService, CollectionListService sourceUserCollections)
		{
			_progressRenderer = progressRenderer;
			_collStatsService = collStatsService;
			_sourceUserCollections = sourceUserCollections;
		}

		public async Task<CollStatOfAllUserCollections> Get(CancellationToken token)
		{
			var userCollections = await _sourceUserCollections.Get(token);

			await using var reporter = _progressRenderer.Start("Load collection statistics", userCollections.Count);

			async Task<KeyValuePair<CollectionNamespace, CollectionStatistics>> runCollStats(CollectionNamespace ns, CancellationToken t)
			{
				var collStats = await _collStatsService.Get(ns, t);
				reporter.Increment();
				return new KeyValuePair<CollectionNamespace, CollectionStatistics>(ns, collStats);
			}

			var allCollStats = await userCollections.ParallelsAsync(runCollStats, 32, token);

			return new CollStatOfAllUserCollections(allCollStats);
		}
	}
}
