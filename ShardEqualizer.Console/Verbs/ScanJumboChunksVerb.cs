using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Ninject;
using ShardEqualizer.Operations;

namespace ShardEqualizer.Verbs
{
	[Verb("scanJumbo", HelpText = "Scan jumbo chunks.")]
	public class ScanJumboChunksVerb : BaseVerbose
	{
		protected override async Task RunOperation(IKernel kernel, CancellationToken token)
		{
			kernel.Bind<IOperation>().To<ScanJumboChunksOperation>();

			await base.RunOperation(kernel, token);
		}
	}
}
