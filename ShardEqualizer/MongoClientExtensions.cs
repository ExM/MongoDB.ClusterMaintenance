using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace ShardEqualizer
{
	public static class MongoClientExtensions
	{
		public static async Task<IReadOnlyCollection<string>> ListUserDatabases(this IMongoClient mongoClient, CancellationToken token)
		{
			var allDatabaseNames = await mongoClient.ListDatabaseNames().ToListAsync(token);
			return allDatabaseNames.Except(new[] {"admin", "config"}).ToList();
		}
	}
}
