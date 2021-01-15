using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace ShardEqualizer
{
	public class UserDatabasesSource : IDataSource<UserDatabases>
	{
		private readonly IMongoClient _mongoClient;
		private readonly ProgressRenderer _progressRenderer;

		public UserDatabasesSource(IMongoClient mongoClient, ProgressRenderer progressRenderer)
		{
			_mongoClient = mongoClient;
			_progressRenderer = progressRenderer;
		}

		public async Task<UserDatabases> Get(CancellationToken token)
		{
			await using var reporter = _progressRenderer.Start("Load user databases");
			var result = new UserDatabases(await _mongoClient.ListUserDatabases(token));
			reporter.SetCompleteMessage($"found {result.Count} databases.");
			return result;
		}
	}
}