using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using NConfiguration;
using NConfiguration.Combination;
using NConfiguration.Joining;
using NConfiguration.Variables;
using NConfiguration.Xml;
using Ninject;
using NLog;
using ShardEqualizer.Config;
using ShardEqualizer.Reporting;

namespace ShardEqualizer
{
	internal static class Program
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();

		static async Task<int> Main(string[] args)
		{
			var cts = new CancellationTokenSource();

			Console.CancelKeyPress += (sender, eventArgs) =>
			{
				cts.Cancel();
				eventArgs.Cancel = true;
				_log.Warn("cancel operation requested...");
			};

			var parsed = Parser.Default.ParseArguments(args, loadVerbs()) as Parsed<object>;

			if (parsed == null)
				return 1;

			try
			{
				var verbose = (BaseVerbose) parsed.Value;

				var kernel = new StandardKernel(new NinjectSettings() { LoadExtensions = false });
				kernel.Load<Module>();
				BindConfiguration(verbose, kernel);

				await verbose.Run(kernel, cts.Token);

				return 0;
			}
			catch (Exception e)
			{
				_log.Fatal(e, "unexpected exception");
				Console.Error.WriteLine();
				Console.Error.WriteLine(e.Message);
				return 1;
			}
			finally
			{
				LogManager.Flush();
			}
		}

		private	static Type[] loadVerbs() => Assembly.GetExecutingAssembly().GetTypes()
				.Where(t => t.GetCustomAttribute<VerbAttribute>() != null && !t.IsAbstract).ToArray();

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

			var clusterConfig = loadClusterConfig(appSettings, verbose.ClusterName);

			kernel.Bind<ClusterConfig>().ToConstant(clusterConfig);

			var localStoreConfig = appSettings.TryGet<LocalStoreConfig>() ?? new LocalStoreConfig();
			if (verbose.ResetStore)
				localStoreConfig.ResetStore = true;

			kernel.Bind<LocalStoreConfig>().ToConstant(localStoreConfig);

			var intervalConfigs = loadIntervalConfigurations(clusterConfig, appSettings);
			var intervals = intervalConfigs.Select(_ => new Interval(_)).ToList().AsReadOnly();

			kernel.Bind<IReadOnlyList<Interval>>().ToConstant(intervals);

			kernel.Bind<LayoutStore>()
				.ToMethod(ctx => new LayoutStore(appSettings.TryGet<DeviationLayoutsConfig>().Layouts))
				.InSingletonScope();
		}

		private static ClusterConfig loadClusterConfig(IAppSettings settings, string clusterNameInCommandLine)
		{
			var selectedCluster = clusterNameInCommandLine ?? settings.TryGet<ClustersConfig>()?.Default;
			if (string.IsNullOrWhiteSpace(selectedCluster))
				throw new Exception("can't select cluster name in configuration (Clusters/Default) or command line");

			return settings.Subsection(AppSettingExtensions.GetSectionName<ClustersConfig>())
				.Get<ClusterConfig>(selectedCluster);
		}

		private static IEnumerable<IntervalConfig> loadIntervalConfigurations(ClusterConfig clusterConfig, ICombiner combiner)
		{
			foreach (var g in clusterConfig.Intervals.GroupBy(_ => _.Namespace, StringComparer.Ordinal))
			{
				var intervalConfig = g.Aggregate(combiner.Combine);
				intervalConfig.Zones ??= clusterConfig.Zones;

				yield return intervalConfig;
			}
		}
	}
}
