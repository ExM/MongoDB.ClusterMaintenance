using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.ClusterMaintenance.Config;
using MongoDB.ClusterMaintenance.MongoCommands;
using MongoDB.ClusterMaintenance.Reporting;
using MongoDB.ClusterMaintenance.Verbs;
using MongoDB.ClusterMaintenance.WorkFlow;
using MongoDB.Driver;
using NLog;

namespace MongoDB.ClusterMaintenance.Operations
{
	public class DeviationOperation: IOperation
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();
		
		private readonly IMongoClient _mongoClient;
		private readonly ScaleSuffix _scaleSuffix;
		private readonly ReportFormat _reportFormat;

		public DeviationOperation(IMongoClient mongoClient, ScaleSuffix scaleSuffix, ReportFormat reportFormat)
		{
			_mongoClient = mongoClient;
			_scaleSuffix = scaleSuffix;
			_reportFormat = reportFormat;
		}
		
		private IList<string> _userDatabases;
		private IList<CollectionNamespace> _allCollectionNames;
		private IReadOnlyList<CollStatsResult> _allCollStats;

		private async Task loadUserDatabases(CancellationToken token)
		{
			_userDatabases = await _mongoClient.ListUserDatabases(token);
		}
		
		private ObservableTask loadCollections(CancellationToken token)
		{
			var progress = new Progress(_userDatabases.Count);

			async Task<IEnumerable<CollectionNamespace>> listCollectionNames(string dbName, CancellationToken t)
			{
				try
				{
					var db = _mongoClient.GetDatabase(dbName);
					var collNames = await db.ListCollectionNames().ToListAsync(t);
					return collNames.Select(_ => new CollectionNamespace(dbName, _));
				}
				finally
				{
					progress.Increment();
				}
			}
			
			async Task work()
			{
				var allCollectionNames = await _userDatabases.ParallelsAsync(listCollectionNames, 32, token);
				_allCollectionNames = allCollectionNames.SelectMany(_ => _).ToList();
			}
			
			return new ObservableTask(progress, work());
		}
		
		private ObservableTask loadCollectionStatistics(CancellationToken token)
		{
			var progress = new Progress(_allCollectionNames.Count);

			async Task<CollStatsResult> runCollStats(CollectionNamespace ns, CancellationToken t)
			{
				try
				{
					var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
					var collStats = await db.CollStats(ns.CollectionName, 1, t);
					return collStats;
				}
				finally
				{
					progress.Increment();
				}
			}
			
			async Task work()
			{
				_allCollStats = await _allCollectionNames.ParallelsAsync(runCollStats, 32, token);
			}

			return new ObservableTask(progress, work());
		}
		
		public async Task Run(CancellationToken token)
		{
			var opList = new OperationList()
			{
				new SingleWork("Load user databases", loadUserDatabases, () => $"found {_userDatabases.Count} databases."),
				new ObservableWork("Load collections", loadCollections, () => $"found {_allCollectionNames.Count} collections."),
				new ObservableWork("Load collection statistics", loadCollectionStatistics),
			};

			await opList.Apply("", token);
			
			var sizeRenderer = new SizeRenderer("F2", _scaleSuffix);

			var report = createReport(sizeRenderer);
			foreach (var collStats in _allCollStats)
			{
				report.Append(collStats);
			}
			report.CalcBottom();
			
			var sb = report.Render();
			
			Console.WriteLine("Report as CSV:");
			Console.WriteLine();
			Console.WriteLine(sb);
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