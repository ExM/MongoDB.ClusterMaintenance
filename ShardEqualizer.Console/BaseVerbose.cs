using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Ninject;

namespace ShardEqualizer
{
	public abstract class BaseVerbose
	{
		[Option('f', "config", Required = false, HelpText = "configuration file", Default = "configuration.xml")]
		public string ConfigFile { get; set; }

		[Option('c', "clusterName", Required = false,  HelpText = "selected cluster name in configuration file")]
		public string ClusterName { get; set; }

		public abstract Task RunOperation(IKernel kernel, CancellationToken token);
	}
}
