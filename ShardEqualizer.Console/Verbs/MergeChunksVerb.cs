using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Ninject;
using ShardEqualizer.Operations;

namespace ShardEqualizer.Verbs
{
	[Verb("merge", HelpText = "Merge empty or small chunks.")]
	public class MergeChunksVerb: BaseCommandFileVerb
	{
		protected override async Task RunOperation(IKernel kernel, CancellationToken token)
		{
			kernel.Bind<IOperation>().To<MergeChunksOperation>();

			await base.RunOperation(kernel, token);
		}
	}
}
