using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Ninject;
using ShardEqualizer.Operations;

namespace ShardEqualizer.Verbs
{
	[Verb("equalize", HelpText = "Alignment shard sizes by moving bound of zones.")]
	public class EqualizeVerb: BaseCommandFileVerb
	{
		[Option("moveLimit", Required = false, Default = null, HelpText = "limit of moving data (Mb)")]
		public long? MoveLimit { get; set; }

		[Option("planOnly", Required = false, Default = false, HelpText = "show moving plan without equalization")]
		public bool PlanOnly { get; set; }

		protected override Task RunOperation(IKernel kernel, CancellationToken token)
		{
			long? scaledMoveLimit = null;
			if (MoveLimit.HasValue)
				scaledMoveLimit = MoveLimit.Value * ScaleSuffix.Mega.Factor();

			kernel.Bind<IOperation>().To<EqualizeOperation>()
				.WithConstructorArgument(scaledMoveLimit)
				.WithConstructorArgument(PlanOnly);

			return base.RunOperation(kernel, token);
		}
	}
}
