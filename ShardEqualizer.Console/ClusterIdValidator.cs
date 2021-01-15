using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using NLog;
using ShardEqualizer.Config;

namespace ShardEqualizer
{
	public class ClusterIdValidator
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();
		private readonly ClusterConfig _clusterConfig;
		private readonly IConfigDbRepositoryProvider _configDb;
		private readonly ProgressRenderer _progressRenderer;

		public ClusterIdValidator(ClusterConfig clusterConfig, IConfigDbRepositoryProvider configDb, ProgressRenderer progressRenderer)
		{
			_clusterConfig = clusterConfig;
			_configDb = configDb;
			_progressRenderer = progressRenderer;
		}

		public async Task Validate()
		{
			if (_clusterConfig.Id == null)
				return;

			await using (_progressRenderer.Start($"Check cluster id {_clusterConfig.Id}"))
			{
				var clusterIdFromConfig = ObjectId.Parse(_clusterConfig.Id);
				var clusterIdFromConnection = await _configDb.Version.GetClusterId();

				if (clusterIdFromConfig != clusterIdFromConnection)
					throw new Exception(
						$"Cluster id mismatch! Expected {clusterIdFromConfig}, but current connection returned {clusterIdFromConnection}");
			}
		}
	}
}
