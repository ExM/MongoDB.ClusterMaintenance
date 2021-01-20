using System.Linq;
using MongoDB.Driver;
using NLog;
using ShardEqualizer.Config;

namespace ShardEqualizer
{
	public class MongoClientBuilder
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();
		private readonly ClusterConfig _clusterConfig;
		private readonly ProgressRenderer _progressRenderer;

		public MongoClientBuilder(ClusterConfig clusterConfig, ProgressRenderer progressRenderer)
		{
			_clusterConfig = clusterConfig;
			_progressRenderer = progressRenderer;
		}

		public IMongoClient Build()
		{
			_progressRenderer.WriteLine($"Connecting to {string.Join(",", _clusterConfig.Servers)}");

			_log.Info("Connecting to {0}", string.Join(",", _clusterConfig.Servers));

			var urlBuilder = new MongoUrlBuilder()
			{
				Servers = _clusterConfig.Servers.Select(MongoServerAddress.Parse),
			};

			if(_clusterConfig.IsRequireAuth)
			{
				urlBuilder.AuthenticationSource = "admin";
				urlBuilder.Username = _clusterConfig.User;
				urlBuilder.Password = _clusterConfig.Password;
			}

			var settings = MongoClientSettings.FromUrl(urlBuilder.ToMongoUrl());
			settings.ClusterConfigurator += CommandLogger.Subscriber;
			settings.ReadPreference = ReadPreference.SecondaryPreferred;
			settings.MinConnectionPoolSize = 32;
			settings.ApplicationName = GetType().Assembly.GetName().ToString();
			return new MongoClient(settings);
		}
	}
}
