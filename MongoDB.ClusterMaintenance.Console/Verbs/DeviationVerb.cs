using System;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using MongoDB.ClusterMaintenance.Operations;
using Ninject;

namespace MongoDB.ClusterMaintenance.Verbs
{
	[Verb("deviation", HelpText = "Calculation of collection size deviation")]
	public class DeviationVerb: BaseVerbose
	{
		[Option('s', "scale", Required = false, Default = "", HelpText = "scale of size (K,M,G,T,P,E)")]
		public string Scale { get; set; }

		[Option("format", Required = false, Default = "csv", HelpText = "format of report (csv - CSV, md - markdown")]
		public string Format { get; set; }

		public override async Task RunOperation(IKernel kernel, CancellationToken token)
		{
			kernel.Bind<IOperation>().To<DeviationOperation>()
				.WithConstructorArgument(parseScaleSuffix(Scale))
				.WithConstructorArgument(parseReportFormat(Format));
			
			await kernel.Get<IOperation>().Run(token);
		}

		private ScaleSuffix parseScaleSuffix(string scale)
		{
			switch (scale)
			{
				case "": return ClusterMaintenance.ScaleSuffix.None;
				case "K": return ClusterMaintenance.ScaleSuffix.Kilo;
				case "M": return ClusterMaintenance.ScaleSuffix.Mega;
				case "G": return ClusterMaintenance.ScaleSuffix.Giga;
				case "T": return ClusterMaintenance.ScaleSuffix.Tera;
				case "P": return ClusterMaintenance.ScaleSuffix.Peta;
				case "E": return ClusterMaintenance.ScaleSuffix.Exa;
				default:
					throw new FormatException($"unexpected text '{scale}' in the scale option");
			}
		}
		
		private ReportFormat parseReportFormat(string scale)
		{
			switch (scale)
			{
				case "csv": return ReportFormat.Csv;
				case "md": return ReportFormat.Markdown;
				default:
					throw new FormatException($"unexpected text '{scale}' in the format option");
			}
		}
	}

	public enum ReportFormat
	{
		Csv,
		Markdown
	}
}