using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using NLog;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.Reporting;
using ShardEqualizer.Verbs;
using ShardEqualizer.WorkFlow;

namespace ShardEqualizer.Operations
{
	public class DeviationOperation: IOperation
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();

		private readonly IDataSource<CollStatOfUserCollections> _collStatSource;
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly ScaleSuffix _scaleSuffix;
		private readonly ReportFormat _reportFormat;
		private readonly List<LayoutDescription> _layouts;

		public DeviationOperation(
			IDataSource<CollStatOfUserCollections> collStatSource,
			IReadOnlyList<Interval> intervals,
			ScaleSuffix scaleSuffix,
			ReportFormat reportFormat,
			List<LayoutDescription> layouts)
		{
			_collStatSource = collStatSource;
			_intervals = intervals;
			_scaleSuffix = scaleSuffix;
			_reportFormat = reportFormat;
			_layouts = layouts;
		}

		public async Task Run(CancellationToken token)
		{
			var allCollStats = await _collStatSource.Get(token);

			var sizeRenderer = new SizeRenderer("F2", _scaleSuffix);

			var report = createReport(sizeRenderer);
			foreach (var collStats in allCollStats.Values)
			{
				var interval = _intervals.FirstOrDefault(_ => _.Namespace.FullName == collStats.Ns.FullName);
				report.Append(collStats, interval?.Correction);
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
