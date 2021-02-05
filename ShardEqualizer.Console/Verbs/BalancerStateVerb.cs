using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Ninject;
using ShardEqualizer.Operations;

namespace ShardEqualizer.Verbs
{
	[Verb("balancer", HelpText = "Show state of shards balancer and the end of the movement of shards.")]
	public class BalancerStateVerb: BaseVerbose
	{
		protected override async Task RunOperation(IKernel kernel, CancellationToken token)
		{
			kernel.Bind<IOperation>().To<BalancerStateOperation>();

			await base.RunOperation(kernel, token);
		}
	}
}
