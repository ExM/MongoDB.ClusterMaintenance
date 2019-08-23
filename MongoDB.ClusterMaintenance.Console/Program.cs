using CommandLine;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.ClusterMaintenance.Config;
using MongoDB.ClusterMaintenance.Verbs;
using MongoDB.Driver;
using NConfiguration;
using NConfiguration.Joining;
using NConfiguration.Xml;
using NConfiguration.Combination;
using NConfiguration.Variables;
using NLog;
using Ninject;

namespace MongoDB.ClusterMaintenance
{
	internal static class Program
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();
		
		static int Main(string[] args)
		{
			var cts = new CancellationTokenSource();

			Console.CancelKeyPress += (sender, eventArgs) =>
			{
				cts.Cancel();
				eventArgs.Cancel = true;
				_log.Warn("cancel operation requested...");
			};
			
			var parsed = Parser.Default.ParseArguments<
				FindNewCollectionsVerb,
				ScanChunksVerb,
				MergeChunksVerb,
				PresplitDataVerb,
				BalancerStateVerb,
				DeviationVerb,
				EqualizeVerb>(args) as Parsed<object>;

			if (parsed == null)
				return 1;

			try
			{
				var verbose = (BaseVerbose) parsed.Value;

				var kernel = new StandardKernel(new NinjectSettings() { LoadExtensions = false });
				kernel.Load<Module>();
				BindConfiguration(verbose, kernel);
			
				var result = ProcessVerbAndReturnExitCode(t => verbose.RunOperation(kernel, t), cts.Token).Result;

				foreach(var item in kernel.GetAll<IDisposable>())
					item.Dispose();
				
				return result;
			}
			catch (Exception e)
			{
				_log.Fatal(e, "unexpected exception");
				Console.Error.WriteLine(e.Message);
				return 1;
			}
			finally
			{
				LogManager.Flush();
			}
		}
		
		private static IAppSettings loadConfiguration(string configFile)
		{
			var storage = new VariableStorage();
			var loader = new SettingsLoader(storage.CfgNodeConverter);
			loader.Loaded += (s, e) => _log.Info("Loaded: {0} ({1})", e.Settings.GetType(), e.Settings.Identity);
			loader.XmlFileBySection().FindingSettings += (s, e) => _log.Info("Search '{0}' from '{1}'", e.IncludeFile.Path, e.SearchPath);
			return loader.LoadSettings(new XmlFileSettings(configFile)).Joined.ToAppSettings();
		}

		public static void BindConfiguration(BaseVerbose verbose, IKernel kernel)
		{
			var appSettings = loadConfiguration(verbose.ConfigFile);
			
			var connectionConfig = appSettings.Get<Connection>();
			kernel.Bind<IMongoClient>().ToMethod(_ => createClient(connectionConfig)).InSingletonScope();

			var boundsFileConfig = appSettings.Get<BoundsFile>();
			var bounds = readBoundsFile(boundsFileConfig.Path);

			var intervalConfigs = loadIntervalConfigurations(appSettings);
			var intervals = intervalConfigs.Select(_ => new Interval(_, bounds)).ToList().AsReadOnly();
			
			if (verbose.Database != null)
				foreach (var interval in intervals)
				{
					if (!interval.Namespace.DatabaseNamespace.DatabaseName.Equals(verbose.Database, StringComparison.Ordinal))
						interval.Selected = false;
				}
			
			if(verbose.Collection != null)
				foreach (var interval in intervals)
				{
					if (!interval.Namespace.CollectionName.Equals(verbose.Collection, StringComparison.Ordinal))
						interval.Selected = false;
				}

			if (intervals.Count == 0)
				throw new ArgumentException("interval list is empty");
			
			kernel.Bind<IReadOnlyList<Interval>>().ToConstant(intervals);
		}

		private static IEnumerable<IntervalConfig> loadIntervalConfigurations(IAppSettings settings)
		{
			var nsMap = new Dictionary<string, IntervalConfig>(StringComparer.Ordinal);

			foreach (var g in settings.LoadSections<IntervalConfig>().GroupBy(_ => _.Namespace, StringComparer.Ordinal))
			{
				using (var cfgItem = g.GetEnumerator())
				{
					cfgItem.MoveNext();
					var sumItem = cfgItem.Current;
					while (cfgItem.MoveNext())
						sumItem = settings.Combine(sumItem, cfgItem.Current);
					
					nsMap.Add(g.Key, sumItem);
				}
			}

			if (!nsMap.TryGetValue("default", out var defaultItem))
				return nsMap.Values;
			
			nsMap.Remove("default");
			return nsMap.Values.Select(_ => settings.Combine(defaultItem, _));
		}
		
		public static BsonDocument readBoundsFile(string path)
		{
			if(string.IsNullOrWhiteSpace(path))
				return new BsonDocument();
			
			var jsonReaderSettings = new JsonReaderSettings()
				{GuidRepresentation = GuidRepresentation.Unspecified};
			
			using(var textReader = File.OpenText(path))
			using (var jsonReader = new JsonReader(textReader, jsonReaderSettings))
			{
				var bsonDocument = BsonDocumentSerializer.Instance.Deserialize(BsonDeserializationContext.CreateRoot(jsonReader));
				if (!jsonReader.IsAtEndOfFile())
					throw new FormatException("String contains extra non-whitespace characters beyond the end of the document.");
				return bsonDocument;
			}
		}
		
		private static IMongoClient createClient(Connection connectionConfig)
		{
			_log.Info("Connecting to {0}", string.Join(",", connectionConfig.Servers));

			var urlBuilder = new MongoUrlBuilder()
			{
				Servers = connectionConfig.Servers.Select(MongoServerAddress.Parse),
			};

			if(connectionConfig.IsRequireAuth)
			{
				urlBuilder.AuthenticationSource = "admin";
				urlBuilder.Username = connectionConfig.User;
				urlBuilder.Password = connectionConfig.Password;
			}
			
			var settings = MongoClientSettings.FromUrl(urlBuilder.ToMongoUrl());
			settings.ClusterConfigurator += CommandLogger.Subscriber;
			settings.ReadPreference = ReadPreference.Primary;
			settings.MinConnectionPoolSize = 32;
			return new MongoClient(settings);
		}
		
		private static async Task<int> ProcessVerbAndReturnExitCode(Func<CancellationToken, Task> action, CancellationToken token)
		{
			try
			{
				await action(token);
				return 0;
			}
			catch (Exception e)
			{
				if (!token.IsCancellationRequested)
				{
					_log.Fatal(e, "unexpected exception");
					Console.Error.WriteLine(e.Message);
				}
				
				return 1;
			}
			finally
			{
				LogManager.Flush();
			}
		}
	}
}
