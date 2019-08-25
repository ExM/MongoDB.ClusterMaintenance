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
using MongoDB.ClusterMaintenance.UI;
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
		
		private IList<string> _userDatabases;
		private IList<CollectionNamespace> _allCollectionNames;
		private IReadOnlyList<CollStatsResult> _allCollStats;

		private async Task loadUserDatabases(CancellationToken token)
		{
			_userDatabases = await _mongoClient.ListUserDatabases(token);
		}
		
		private ObservableWork loadCollections(CancellationToken token)
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
			
			return new ObservableWork(progress, work());
		}
		
		private ObservableWork loadCollectionStatistics(CancellationToken token)
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

			return new ObservableWork(progress, work());
		}
		
		public async Task Run(CancellationToken token)
		{
			var opList = new OperationList()
			{
				new SingleOperation2("Load user databases", loadUserDatabases, () => $"found {_userDatabases.Count} databases."),
				new ObservableOperation2("Load collections", loadCollections, () => $"found {_allCollectionNames.Count} collections."),
				new ObservableOperation2("Load collection statistics", loadCollectionStatistics),
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

	public interface IOperation2
	{
		Task Apply(string prefix, CancellationToken token);
	}
	
	public class SingleOperation2: IOperation2
	{
		private readonly Func<CancellationToken, Task> _action;
		private readonly Func<string> _doneMessageRenderer;
		private readonly string _title;

		public SingleOperation2(string title, Func<CancellationToken, Task> action, Func<string> doneMessageRenderer = null)
		{
			_action = action;
			_doneMessageRenderer = doneMessageRenderer;
			_title = title;
		}

		public virtual async Task Apply(string prefix, CancellationToken token)
		{
			Console.Write(prefix);
			Console.Write(_title);
			Console.Write(" ... ");
			await _action(token);

			var doneMessage = _doneMessageRenderer == null ? "done" : _doneMessageRenderer();
			Console.WriteLine(doneMessage);
		}
	}
	
	public class ObservableOperation2: IOperation2
	{
		private readonly Func<CancellationToken, ObservableWork> _action;
		private readonly Func<string> _doneMessageRenderer;
		private readonly string _title;

		public ObservableOperation2(string title, Func<CancellationToken, ObservableWork> action, Func<string> doneMessageRenderer = null)
		{
			_action = action;
			_doneMessageRenderer = doneMessageRenderer;
			_title = title;
		}

		public virtual async Task Apply(string prefix, CancellationToken token)
		{
			Console.Write(prefix);
			Console.Write(_title);
			Console.Write(" ... ");
			var work = _action(token);

			var progress = work.Progress;

			var frame = new ConsoleFrame(builder =>
			{
				progress.Refresh();
				builder.AppendLine("");
				builder.AppendLine(
					$"# Progress: {progress.Completed}/{progress.Total} Elapsed: {progress.Elapset} Left: {progress.Left}");
			});

			var cts = new CancellationTokenSource();

			var cancelProgressLoop = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, token).Token;
			
			var progressTask = Task.Factory.StartNew(() => showProgressLoop(frame, cancelProgressLoop), TaskCreationOptions.LongRunning);

			try
			{
				await work.Work;
			}
			finally
			{
				cts.Cancel();
				await progressTask;
			}

			var doneMessage = _doneMessageRenderer == null ? "done" : _doneMessageRenderer();
			Console.WriteLine(doneMessage);
		}
		
		private async Task showProgressLoop(ConsoleFrame frame, CancellationToken token)
		{
			while (!token.IsCancellationRequested)
			{
				frame.Refresh();

				try
				{
					await Task.Delay(100, token);
				}
				catch (TaskCanceledException)
				{
					break;
				}
			}
			
			frame.Clear();
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

		public async Task Apply(string prefix, CancellationToken token)
		{
			Console.Write(prefix);
			Console.WriteLine(_title ?? "");

			foreach (var item in _opList.Select((operation, order) => new {operation, order}))
			{
				await item.operation.Apply($"{item.order + 1}/{_opList.Count} ", token);
			}
		}

		public IEnumerator GetEnumerator()
		{
			throw new NotImplementedException();
		}
	}
}