using System;
using System.Linq.Expressions;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.ClusterMaintenance.Config;
using MongoDB.ClusterMaintenance.MongoCommands;
using MongoDB.ClusterMaintenance.Reporting;
using MongoDB.ClusterMaintenance.Verbs;
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

		private async Task showProgressLoop(Progress progress, CancellationToken token)
		{
			while (!token.IsCancellationRequested)
			{
				progress.Refresh();
				_log.Info("# Progress {0}/{1} Elapsed: {2} Left: {3}", progress.Completed, progress.Total, progress.Elapset, progress.Left);
				await Task.Delay(250);
			}
		}
		
		public async Task Run(CancellationToken token)
		{
			_log.Info("Begin operation");
			var listCollProgress = new Progress();
			var allCollectionNamesTask = _mongoClient.ListUserCollections(listCollProgress, token);

			var cts = new CancellationTokenSource();

			var listProgressTask = showProgressLoop(listCollProgress, cts.Token);
			var allCollectionNames = await allCollectionNamesTask;
			cts.Cancel();
			await listProgressTask;
			
			_log.Info("Found: {0} collections", allCollectionNames.Count);
			
			var statCollProgress = new Progress();
			statCollProgress.Start(allCollectionNames.Count);

			async Task<CollStatsResult> runCollStats(CollectionNamespace ns, CancellationToken innerToken)
			{
				try
				{
					_log.Info("collection: {0}", ns);
					var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
					var collStats = await db.CollStats(ns.CollectionName, 1, innerToken);
					return collStats;
				}
				finally
				{
					statCollProgress.Increment();
				}
			}
			
			var collStatsTask = allCollectionNames.ParallelsAsync(runCollStats, 32, token);
			var cts2 = new CancellationTokenSource();
			var statsProgressTask = showProgressLoop(statCollProgress, cts2.Token);
			
			var result = await collStatsTask;
			cts2.Cancel();
			await statsProgressTask;
			
			var sizeRenderer = new SizeRenderer("F2", _scaleSuffix);

			var report = createReport(sizeRenderer);
			foreach (var collStats in result)
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