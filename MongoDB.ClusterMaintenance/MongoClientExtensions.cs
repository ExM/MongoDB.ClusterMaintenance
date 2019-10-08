using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.ClusterMaintenance.WorkFlow;
using MongoDB.Driver;

namespace MongoDB.ClusterMaintenance
{
	public static class MongoClientExtensions
	{
		public static async Task<IList<string>> ListUserDatabases(this IMongoClient mongoClient, CancellationToken token)
		{
			var allDatabaseNames = await mongoClient.ListDatabaseNames().ToListAsync(token);
			return allDatabaseNames.Except(new[] {"admin", "config"}).ToList();
		}
		
		public static async Task<IList<CollectionNamespace>> ListUserCollections(this IMongoClient mongoClient, CancellationToken token)
		{
			var userDataBases = await mongoClient.ListUserDatabases(token);
			var progress = new Progress(userDataBases.Count);
			return await mongoClient.ListUserCollections(userDataBases, progress, token);
		}
		
		public static async Task<IList<CollectionNamespace>> ListUserCollections(this IMongoClient mongoClient, IList<string> userDataBases, Progress progress, CancellationToken token)
		{
			async Task<IEnumerable<CollectionNamespace>> listCollectionNames(string dbName, CancellationToken t)
			{
				try
				{
					var db = mongoClient.GetDatabase(dbName);
					var collNames = await db.ListCollectionNames().ToListAsync(t);
					return collNames.Select(_ => new CollectionNamespace(dbName, _));
				}
				finally
				{
					progress.Increment();
				}
			}
			
			return (await userDataBases.ParallelsAsync(listCollectionNames, 32, token)).SelectMany(_ => _).ToList();
		}
	}
}