using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Ninject;
using ShardEqualizer.Operations;

namespace ShardEqualizer.Verbs
{
	[Verb("findNewCollections", HelpText = "Scan new sharded collections and create default configuration")]
	public class FindNewCollectionsVerb: BaseVerbose
	{
		public override async Task RunOperation(IKernel kernel, CancellationToken token)
		{
			kernel.Bind<IOperation>().To<FindNewCollectionsOperation>();

			await kernel.Get<IOperation>().Run(token);
		}
	}
}