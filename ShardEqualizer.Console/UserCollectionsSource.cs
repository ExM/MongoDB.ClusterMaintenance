using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace ShardEqualizer
{
	public class UserCollectionsSource : IDataSource<UserCollections>
	{
		private readonly IMongoClient _mongoClient;
		private readonly IDataSource<UserDatabases> _sourceUserDatabases;
		private readonly ProgressRenderer _progressRenderer;

		public UserCollectionsSource(IMongoClient mongoClient, IDataSource<UserDatabases> sourceUserDatabases, ProgressRenderer progressRenderer)
		{
			_mongoClient = mongoClient;
			_sourceUserDatabases = sourceUserDatabases;
			_progressRenderer = progressRenderer;
		}

		public async Task<UserCollections> Get(CancellationToken token)
		{
			var userDatabases = await _sourceUserDatabases.Get(token);

			await using var reporter = _progressRenderer.Start("Load collections", userDatabases.Count);

			async Task<IEnumerable<CollectionNamespace>> listCollectionNames(DatabaseNamespace dbName, CancellationToken t)
			{
				var colls = await _mongoClient.GetDatabase(dbName.DatabaseName).ListUserCollections(t);
				reporter.Increment();
				return colls;
			}

			var results = new UserCollections((await userDatabases.ParallelsAsync(listCollectionNames, 32, token)).SelectMany(_ => _));
			reporter.SetCompleteMessage($"found {results.Count} collections.");
			return results;
		}
	}
}