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

		private readonly IMongoClient _mongoClient;
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly ScaleSuffix _scaleSuffix;
		private readonly ReportFormat _reportFormat;
		private readonly List<LayoutDescription> _layouts;

		public DeviationOperation(IMongoClient mongoClient,  IReadOnlyList<Interval> intervals, ScaleSuffix scaleSuffix, ReportFormat reportFormat, List<LayoutDescription> layouts)
		{
			_mongoClient = mongoClient;
			_intervals = intervals;
			_scaleSuffix = scaleSuffix;
			_reportFormat = reportFormat;
			_layouts = layouts;
		}

		private IReadOnlyCollection<string> _userDatabases;
		private IReadOnlyCollection<CollectionNamespace> _allCollectionNames;
		private IReadOnlyList<CollStatsResult> _allCollStats;

		private async Task loadUserDatabases(CancellationToken token)
		{
			_userDatabases = await _mongoClient.ListUserDatabases(token);
		}

		private ObservableTask loadCollections(CancellationToken token)
		{
			async Task<IEnumerable<CollectionNamespace>> listCollectionNames(string dbName, CancellationToken t)
			{
				return await _mongoClient.GetDatabase(dbName).ListUserCollections(t);
			}

			return ObservableTask.WithParallels(
				_userDatabases,
				32,
				listCollectionNames,
				allCollectionNames => { _allCollectionNames = allCollectionNames.SelectMany(_ => _).ToList(); },
				token);
		}

		private ObservableTask loadCollectionStatistics(CancellationToken token)
		{
			async Task<CollStatsResult> runCollStats(CollectionNamespace ns, CancellationToken t)
			{
				var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
				var collStats = await db.CollStats(ns.CollectionName, 1, t);
				return collStats;
			}

			return ObservableTask.WithParallels(
				_allCollectionNames,
				32,
				runCollStats,
				allCollStats => { _allCollStats = allCollStats; },
				token);
		}

		public async Task Run(CancellationToken token)
		{
			var opList = new WorkList()
			{
				{ "Load user databases", new SingleWork(loadUserDatabases, () => $"found {_userDatabases.Count} databases.")},
				{ "Load collections", new ObservableWork(loadCollections, () => $"found {_allCollectionNames.Count} collections.")},
				{ "Load collection statistics", new ObservableWork(loadCollectionStatistics)},
			};

			await opList.Apply(token);

			var sizeRenderer = new SizeRenderer("F2", _scaleSuffix);

			var report = createReport(sizeRenderer);
			foreach (var collStats in _allCollStats)
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
