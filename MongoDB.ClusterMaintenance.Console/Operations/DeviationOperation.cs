using System;
using System.Collections;
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
				//_log.Info("# Progress {0}/{1} Elapsed: {2} Left: {3}", progress.Completed, progress.Total, progress.Elapset, progress.Left);
				
				Console.WriteLine("# Progress: {0}/{1} Elapsed: {2} Left: {3}", progress.Completed, progress.Total, progress.Elapset, progress.Left);
				
				await Task.Delay(100);
			}
		}
		
		private IList<string> _userDatabases;
		private IList<CollectionNamespace> _allCollectionNames;
		private IReadOnlyList<CollStatsResult> _allCollStats;

		private async Task loadUserDatabases(CancellationToken token)
		{
			_userDatabases = await _mongoClient.ListUserDatabases(token);
		}
		
		private async Task loadCollections(CancellationToken token)
		{
			var progress = new Progress(_userDatabases.Count);
			var cts = new CancellationTokenSource();
			
			var progressTask = Task.Factory.StartNew(() => showProgressLoop(progress, cts.Token), TaskCreationOptions.LongRunning);

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

			var allCollectionNames = await _userDatabases.ParallelsAsync(listCollectionNames, 32, token);

			cts.Cancel();
			await progressTask;
			
			_allCollectionNames = allCollectionNames.SelectMany(_ => _).ToList();
		}
		
		private async Task loadCollectionStatistics(CancellationToken token)
		{
			var progress = new Progress(_allCollectionNames.Count);
			var cts = new CancellationTokenSource();
			
			var progressTask = Task.Factory.StartNew(() => showProgressLoop(progress, cts.Token), TaskCreationOptions.LongRunning);

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

			_allCollStats = await _allCollectionNames.ParallelsAsync(runCollStats, 32, token);

			cts.Cancel();
			await progressTask;
		}
		
		public async Task Run(CancellationToken token)
		{
			var opList = new OperationList()
			{
				new SingleOperation2("Load user databases", () => loadUserDatabases(token), () => $"found {_userDatabases.Count} databases."),
				new SingleOperation2("Load collections", () => loadCollections(token), () => $"found {_allCollectionNames.Count} collections."),
				new SingleOperation2("Load collection statistics", () => loadCollectionStatistics(token)),
			};

			await opList.Apply();
			
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

	public interface IOperation2
	{
		Task Apply();
	}
	
	public class SingleOperation2: IOperation2
	{
		private readonly Func<Task> _action;
		private readonly Func<string> _doneMessageRenderer;
		private readonly string _title;

		public SingleOperation2(string title, Func<Task> action, Func<string> doneMessageRenderer = null)
		{
			_action = action;
			_doneMessageRenderer = doneMessageRenderer;
			_title = title;
		}

		public virtual async Task Apply()
		{
			Console.Write(_title);
			Console.WriteLine(" ...");
			await _action();

			var doneMessage = _doneMessageRenderer == null ? "done" : _doneMessageRenderer();
			Console.WriteLine(doneMessage);
		}
	}
	
	public class OperationList: IOperation2, System.Collections.IEnumerable
	{
		private readonly string _title;
		
		private readonly List<IOperation2> _opList = new List<IOperation2>();

		public OperationList(string title = null)
		{
			_title = title;
		}

		public void Add(IOperation2 operation)
		{
			_opList.Add(operation);
		}

		public async Task Apply()
		{
			if(_title != null)
				Console.WriteLine(_title);

			foreach (var item in _opList.Select((operation, order) => new {operation, order}))
			{
				Console.WriteLine("{0}/{1}", item.order + 1, _opList.Count);
				await item.operation.Apply();
			}
		}

		public IEnumerator GetEnumerator()
		{
			throw new NotImplementedException();
		}
	}
}