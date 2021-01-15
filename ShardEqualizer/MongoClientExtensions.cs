using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace ShardEqualizer
{
	public static class MongoClientExtensions
	{
		public static async Task<IReadOnlyCollection<DatabaseNamespace>> ListUserDatabases(this IMongoClient mongoClient, CancellationToken token)
		{
			var allDatabaseNames = await (await mongoClient.ListDatabaseNamesAsync(token)).ToListAsync(token);
			return allDatabaseNames.Except(new[] {"admin", "config"}).Select(_ => new DatabaseNamespace(_)).ToList();
		}
	}
}
