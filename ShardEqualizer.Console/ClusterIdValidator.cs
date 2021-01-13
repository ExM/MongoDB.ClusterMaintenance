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

		public ClusterIdValidator(ClusterConfig clusterConfig, IConfigDbRepositoryProvider configDb)
		{
			_clusterConfig = clusterConfig;
			_configDb = configDb;
		}

		public async Task Validate()
		{
			if (_clusterConfig.Id == null)
				return;

			var clusterIdFromConfig = ObjectId.Parse(_clusterConfig.Id);
			var clusterIdFromConnection = await _configDb.Version.GetClusterId();

			if (clusterIdFromConfig != clusterIdFromConnection)
				throw new Exception($"Cluster id mismatch! Expected {clusterIdFromConfig}, but current connection returned {clusterIdFromConnection}");
		}
	}
}