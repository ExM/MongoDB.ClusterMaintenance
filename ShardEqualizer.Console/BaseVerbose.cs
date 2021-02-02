using System;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Ninject;
using Ninject.Syntax;
using NLog;
using ShardEqualizer.LocalStoring;

namespace ShardEqualizer
{
	public abstract class BaseVerbose
	{
		[Option('f', "config", Required = false, HelpText = "configuration file", Default = "configuration.xml")]
		public string ConfigFile { get; set; }

		[Option('c', "clusterName", Required = false,  HelpText = "selected cluster name in configuration file")]
		public string ClusterName { get; set; }

		[Option("resetStore", Required = false,  Default = false, HelpText = "clean up of current intermediate storage")]
		public bool ResetStore { get; set; }

		[Option("offline", Required = false,  Default = false, HelpText = "skip read or check cluster id")]
		public bool Offline { get; set; }

		public async Task Run(IKernel kernel, CancellationToken token)
		{
			try
			{
				await kernel.Get<ClusterIdService>().Validate(Offline);

				await RunOperation(kernel, token);
			}
			catch (Exception)
			{
				if (token.IsCancellationRequested)
					return;

				throw;
			}
			finally
			{
				if(!Offline)
					kernel.Get<LocalStoreProvider>().SaveFile();

				foreach (var item in kernel.GetAll<IAsyncDisposable>())
					await item.DisposeAsync();

				LogManager.Flush();
			}
		}


		protected virtual async Task RunOperation(IKernel kernel, CancellationToken token)
		{
			await kernel.Get<IOperation>().Run(token);
		}
	}
}
