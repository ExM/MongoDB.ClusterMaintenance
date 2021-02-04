using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ShardEqualizer.Reporting;
using ShardEqualizer.Verbs;

namespace ShardEqualizer.Operations
{
	public class DeviationOperation: IOperation
	{
		private readonly CollectionListService _collectionListService;
		private readonly CollectionStatisticService _collectionStatisticService;
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly ScaleSuffix _scaleSuffix;
		private readonly ReportFormat _reportFormat;
		private readonly List<LayoutDescription> _layouts;

		public DeviationOperation(
			CollectionListService collectionListService,
			CollectionStatisticService collectionStatisticService,
			IReadOnlyList<Interval> intervals,
			ScaleSuffix scaleSuffix,
			ReportFormat reportFormat,
			List<LayoutDescription> layouts)
		{
			_collectionListService = collectionListService;
			_collectionStatisticService = collectionStatisticService;
			_intervals = intervals;
			_scaleSuffix = scaleSuffix;
			_reportFormat = reportFormat;
			_layouts = layouts;
		}

		public async Task Run(CancellationToken token)
		{
			var userColls = await _collectionListService.Get(token);
			var allCollStats = await _collectionStatisticService.Get(userColls, token);

			var sizeRenderer = new SizeRenderer("F2", _scaleSuffix);

			var report = createReport(sizeRenderer);
			foreach (var (ns, collStats) in allCollStats)
			{
				var interval = _intervals.FirstOrDefault(_ => _.Namespace.FullName == ns.FullName);
				report.Append(collStats, interval?.Adjustable);
			}

			Console.WriteLine($"Report as {_reportFormat}:");
			Console.WriteLine();

			foreach (var layout in _layouts)
			{
				Console.WriteLine($"{layout.Title}:");
				Console.WriteLine(report.Render(layout.Columns));
				Console.WriteLine();
			}
		}

		private BaseReport createReport(SizeRenderer sizeRenderer)
		{
			switch (_reportFormat)
			{
				case ReportFormat.Csv:
					return new CsvReport(sizeRenderer);

				case ReportFormat.Markdown:
					return new MarkdownReport(sizeRenderer);

				default:
					throw new ArgumentException($"unexpected report format: {_reportFormat}");
			}
		}
	}
}
