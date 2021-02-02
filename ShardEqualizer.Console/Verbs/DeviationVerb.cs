using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Ninject;
using ShardEqualizer.Operations;
using ShardEqualizer.Reporting;

namespace ShardEqualizer.Verbs
{
	[Verb("deviation", HelpText = "Calculation of collection size deviation")]
	public class DeviationVerb: BaseVerbose
	{
		[Option('s', "scale", Required = false, Default = "", HelpText = "scale of size (K,M,G,T,P,E)")]
		public string Scale { get; set; }

		[Option("layouts", Required = false, Default = "default", HelpText = "Сomma separated name list of layouts (default)")]
		public string Layouts { get; set; }

		[Option("format", Required = false, Default = "csv", HelpText = "format of report (csv - CSV, md - markdown")]
		public string Format { get; set; }

		protected override async Task RunOperation(IKernel kernel, CancellationToken token)
		{
			var layouts = kernel.Get<LayoutStore>()
				.Get(Layouts.Split(new char[] {','}, StringSplitOptions.RemoveEmptyEntries).Select(_ => _.Trim()))
				.ToList();

			kernel.Bind<IOperation>().To<DeviationOperation>()
				.WithConstructorArgument(parseScaleSuffix(Scale))
				.WithConstructorArgument(parseReportFormat(Format))
				.WithConstructorArgument(layouts);

			await base.RunOperation(kernel, token);
		}

		private ScaleSuffix parseScaleSuffix(string scale)
		{
			switch (scale)
			{
				case "": return ScaleSuffix.None;
				case "K": return ScaleSuffix.Kilo;
				case "M": return ScaleSuffix.Mega;
				case "G": return ScaleSuffix.Giga;
				case "T": return ScaleSuffix.Tera;
				case "P": return ScaleSuffix.Peta;
				case "E": return ScaleSuffix.Exa;
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
