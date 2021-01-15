using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using ShardEqualizer.MongoCommands;

namespace ShardEqualizer
{
	public class CollectionStatisticService
	{
		private readonly IMongoClient _mongoClient;
		private readonly LocalStore _localStore;

		public CollectionStatisticService(IMongoClient mongoClient, LocalStore localStore)
		{
			_mongoClient = mongoClient;
			_localStore = localStore;
		}

		public async Task<CollStatsResult> Get(CollectionNamespace ns, CancellationToken token)
		{
			var result = _localStore.FindCollStats(ns);
			if (result == null)
			{
				var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
				result = await db.CollStats(ns.CollectionName, 1, token);

				_localStore.SaveCollStats(ns, result);
			}

			return result;
		}
	}
}
