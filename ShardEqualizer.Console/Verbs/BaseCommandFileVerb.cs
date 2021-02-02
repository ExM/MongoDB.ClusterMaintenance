using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Ninject;
using NLog;
using ShardEqualizer.MongoCommands;

namespace ShardEqualizer.Verbs
{
	public abstract class BaseCommandFileVerb: BaseVerbose
	{
		[Option("commandFile", Required = false, Default = "", HelpText = "file for command plan")]
		public string CommandFile { get; set; }

		protected override async Task RunOperation(IKernel kernel, CancellationToken token)
		{
			if (string.IsNullOrWhiteSpace(CommandFile))
				CommandFile = $"commandPlan_{DateTime.UtcNow:yyyyMMdd_HHmm}.js";

			var fullPath = Path.GetFullPath(CommandFile);

			await using (var file = File.CreateText(fullPath))
			{
				var commandPlanWriter = new CommandPlanWriter(file);
				kernel.Bind<CommandPlanWriter>().ToConstant(commandPlanWriter);

				await base.RunOperation(kernel, token);
			}

			_log.Info("all commands in file {0}", fullPath);
		}

		private static readonly Logger _log = LogManager.GetCurrentClassLogger();
	}
}
