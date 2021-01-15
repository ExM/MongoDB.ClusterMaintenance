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
		private readonly IMongoClient _mongoClient;
		private readonly IDataSource<UserCollections> _sourceUserCollections;

		public CollStatOfAllUserCollectionsSource(ProgressRenderer progressRenderer, IMongoClient mongoClient, IDataSource<UserCollections> sourceUserCollections)
		{
			_progressRenderer = progressRenderer;
			_mongoClient = mongoClient;
			_sourceUserCollections = sourceUserCollections;
		}

		public async Task<CollStatOfAllUserCollections> Get(CancellationToken token)
		{
			var userCollections = await _sourceUserCollections.Get(token);

			await using var reporter = _progressRenderer.Start("Load collection statistics", userCollections.Count);

			async Task<CollStatsResult> runCollStats(CollectionNamespace ns, CancellationToken t)
			{
				var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
				var collStats = await db.CollStats(ns.CollectionName, 1, t);
				reporter.Increment();
				return collStats;
			}

			var allCollStats = await userCollections.ParallelsAsync(runCollStats, 32, token);

			return new CollStatOfAllUserCollections(allCollStats.ToDictionary(_ => _.Ns));
		}
	}
}
