using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using NLog;
using ShardEqualizer.Config;

namespace ShardEqualizer
{
	public class ClusterIdService
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();
		private readonly ClusterConfig _clusterConfig;
		private readonly VersionRepository _versionRepository;
		private readonly ProgressRenderer _progressRenderer;
		private ObjectId? _clusterId;

		public ClusterIdService(ClusterConfig clusterConfig, VersionRepository versionRepository, ProgressRenderer progressRenderer)
		{
			_clusterConfig = clusterConfig;
			_versionRepository = versionRepository;
			_progressRenderer = progressRenderer;

			if (_clusterConfig.Id != null)
				_clusterId = ObjectId.Parse(_clusterConfig.Id);
		}

		public async Task Validate(bool offline)
		{
			if (offline)
			{
				var copy = ClusterId;
				return;
			}

			if (_clusterId == null)
			{
				await using (_progressRenderer.Start($"Read cluster id {_clusterConfig.Id}"))
				{
					_clusterId = await _versionRepository.GetClusterId();
				}
			}
			else
			{
				await using (_progressRenderer.Start($"Check cluster id {_clusterConfig.Id}"))
				{
					var clusterIdFromConnection = await _versionRepository.GetClusterId();

					if (_clusterId.Value != clusterIdFromConnection)
						throw new Exception(
							$"Cluster id mismatch! Expected {_clusterId.Value}, but current connection returned {clusterIdFromConnection}");
				}
			}
		}

		public ObjectId ClusterId
		{
			get
			{
				if (_clusterId == null)
					throw new Exception("requires setting Clusters/{clusterName}/Id in config file");
				return _clusterId.Value;
			}
		}
	}
}
