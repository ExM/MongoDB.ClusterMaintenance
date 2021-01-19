using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer
{
	public class CollectionStatisticService
	{
		private readonly IMongoClient _mongoClient;
		private readonly CollStatsLocalStore _collStatsLocalStore;

		public CollectionStatisticService(IMongoClient mongoClient, CollStatsLocalStore collStatsLocalStore)
		{
			_mongoClient = mongoClient;
			_collStatsLocalStore = collStatsLocalStore;
		}

		public async Task<CollectionStatistics> Get(CollectionNamespace ns, CancellationToken token)
		{
			var result = _collStatsLocalStore.FindCollStats(ns);
			if (result == null)
			{
				var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
				var collStat = await db.CollStats(ns.CollectionName, 1, token);

				result = new CollectionStatistics(collStat);
				_collStatsLocalStore.SaveCollStats(ns, result);
			}

			return result;
		}
	}
}
