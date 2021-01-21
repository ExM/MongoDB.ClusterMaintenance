using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using NLog;
using ShardEqualizer.Config;
using ShardEqualizer.Models;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.ShardSizeEqualizing;
using ShardEqualizer.ShortModels;
using ShardEqualizer.WorkFlow;

namespace ShardEqualizer.Operations
{
	public class EqualizeOperation: IOperation
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();

		private readonly IReadOnlyList<Interval> _intervals;
		private readonly ShardListService _shardListService;
		private readonly CollectionListService _collectionListService;
		private readonly CollectionStatisticService _collectionStatisticService;
		private readonly ShardedCollectionService _shardedCollectionService;
		private readonly TagRangeService _tagRangeService;
		private readonly ClusterSettingsService _clusterSettingsService;
		private readonly ChunkService _chunkService;
		private readonly IMongoClient _mongoClient;
		private readonly ProgressRenderer _progressRenderer;
		private readonly CommandPlanWriter _commandPlanWriter;
		private readonly long? _moveLimit;
		private readonly DebugDirectory _debugDirectory;
		private readonly bool _planOnly;

		public EqualizeOperation(
			ShardListService shardListService,
			CollectionListService collectionListService,
			CollectionStatisticService collectionStatisticService,
			ShardedCollectionService shardedCollectionService,
			TagRangeService tagRangeService,
			ClusterSettingsService clusterSettingsService,
			ChunkRepository chunkRepo,
			ChunkService chunkService,
			IReadOnlyList<Interval> intervals,
			IMongoClient mongoClient,
			ProgressRenderer progressRenderer,
			CommandPlanWriter commandPlanWriter,
			long? moveLimit,
			DebugDirectory debugDirectory,
			bool planOnly)
		{
			_shardListService = shardListService;
			_collectionListService = collectionListService;
			_collectionStatisticService = collectionStatisticService;
			_shardedCollectionService = shardedCollectionService;
			_tagRangeService = tagRangeService;
			_clusterSettingsService = clusterSettingsService;
			_chunkService = chunkService;
			_mongoClient = mongoClient;
			_progressRenderer = progressRenderer;
			_commandPlanWriter = commandPlanWriter;
			_moveLimit = moveLimit;
			_debugDirectory = debugDirectory;
			_planOnly = planOnly;

			if (intervals.Count == 0)
				throw new ArgumentException("interval list is empty");

			_intervals = intervals;
			_adjustableIntervals = _intervals
				.Where(_ => _.Correction != CorrectionMode.None)
				.ToList();
		}

		private IReadOnlyCollection<Shard> _shards;
		private long _chunkSize;
		private IReadOnlyDictionary<CollectionNamespace, CollectionStatistics> _collStatsMap;
		private IReadOnlyDictionary<CollectionNamespace, IReadOnlyList<ChunkInfo>> _chunksByCollection;
		private IReadOnlyDictionary<CollectionNamespace, ShardedCollectionInfo> _shardedCollectionInfoByNs;
		private ZoneOptimizationDescriptor _zoneOpt;
		private Dictionary<TagIdentity, Shard> _shardByTag;
		private IReadOnlyDictionary<CollectionNamespace, IReadOnlyList<TagRange>> _tagRangesByNs;
		private TotalEqualizeReporter _totalEqualizeReporter;
		private readonly IReadOnlyList<Interval> _adjustableIntervals;

		private void createZoneOptimizationDescriptor()
		{
			_progressRenderer.WriteLine($"Analyse of loaded data");

			var unShardedSizeMap = _collStatsMap.Values
				.Where(_ => !_.Sharded)
				.GroupBy(_ => _.Primary.Value)
				.ToDictionary(k => k.Key, g => g.Sum(_ => _.Size));

			_zoneOpt = new ZoneOptimizationDescriptor(
				_adjustableIntervals.Select(_=> _.Namespace),
				_shards.Select(_ => _.Id));

			foreach (var p in unShardedSizeMap)
				_zoneOpt.UnShardedSize[p.Key] = p.Value;

			foreach (var coll in _zoneOpt.Collections)
			{
				if(!_collStatsMap[coll].Sharded)
					continue;

				foreach (var s in _collStatsMap[coll].Shards)
					_zoneOpt[coll, s.Key].CurrentSize = s.Value.Size;
			}

			foreach (var interval in _adjustableIntervals)
			{
				var collCfg = _zoneOpt.CollectionSettings[interval.Namespace];
				collCfg.UnShardCompensation = interval.Correction == CorrectionMode.UnShard;
				collCfg.Priority = interval.Priority;

				var allChunks = _chunksByCollection[interval.Namespace];
				foreach (var tag in interval.Zones)
				{
					var shard = _shardByTag[tag].Id;

					var bucket = _zoneOpt[interval.Namespace, shard];

					bucket.Managed = true;

					var movedChunks = allChunks.Count(_ => _.Shard == shard && !_.Jumbo);
					if (movedChunks <= 1)
						movedChunks = 1;

					bucket.MinSize = bucket.CurrentSize - _chunkSize * (movedChunks - 1);
				}
			}

			var titlePrinted = false;
			foreach (var group in _zoneOpt.AllManagedBuckets.Where(_ => _.CurrentSize == _.MinSize)
				.GroupBy(_ => _.Collection))
			{
				if (!titlePrinted)
				{
					_progressRenderer.WriteLine("\tLock reduction of size:");
					titlePrinted = true;
				}

				_progressRenderer.WriteLine($"\t\t{group.Key} on {string.Join(", ", group.Select(_ => $"{_.Shard} ({_.CurrentSize.ByteSize()})"))}");
			}
		}

		private ChunkCollection createChunkCollection(CollectionNamespace ns, CancellationToken token)
		{
			var collInfo = _shardedCollectionInfoByNs[ns];
			var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);

			async Task<long> chunkSizeResolver(ChunkInfo chunk)
			{
				var result = await db.Datasize(collInfo, chunk, false, token);
				return result.Size;
			}

			return new ChunkCollection(_chunksByCollection[ns], chunkSizeResolver);
		}

		private List<EqualizeWorkItem> findSolution(CancellationToken token)
		{
			_progressRenderer.WriteLine($"Find solution");

			if (_debugDirectory.Enable)
				File.WriteAllText(_debugDirectory.GetFileName("conditionDump", "js"), _zoneOpt.Serialize());

			var solve = ZoneOptimizationSolve.Find(_zoneOpt, token);

			if(!solve.IsSuccess)
				throw new Exception("solution for zone optimization not found");

			var solutionMessage =
				$"Found solution with max deviation {solve.TargetShardMaxDeviation.ByteSize()} by shards";
			_progressRenderer.WriteLine("\t" + solutionMessage);
			_commandPlanWriter.Comment(solutionMessage);

			var titlePrinted = false;
			foreach (var group in solve.ActiveConstraints.GroupBy(_ => _.Bucket.Collection))
			{
				if (!titlePrinted)
				{
					_progressRenderer.WriteLine("\tActive constraint:");
					titlePrinted = true;
				}

				_progressRenderer.WriteLine($"\t\t{group.Key} on {string.Join(", ", group.Select(_ => $"{_.Bucket.Shard} {_.TypeAsText} {_.Bound.ByteSize()}"))}");
			}

			var equalizeWorks = new List<EqualizeWorkItem>();

			_totalEqualizeReporter = new TotalEqualizeReporter(_moveLimit);

			foreach (var interval in _adjustableIntervals)
			{
				var targetSizes = interval.Zones.ToDictionary(
					t => t,
					t => solve[interval.Namespace, _shardByTag[t].Id].TargetSize);

				var equalizer = new ShardSizeEqualizer(
					_shards,
					_collStatsMap[interval.Namespace].Shards,
					_tagRangesByNs[interval.Namespace],
					targetSizes,
					createChunkCollection(interval.Namespace, token));

				equalizer.OnMoveChunk += _totalEqualizeReporter.ChunkMoving;

				_progressRenderer.WriteLine();
				_progressRenderer.WriteLine($"\tEqualize shards from {interval.Namespace}");
				_progressRenderer.WriteLine($"\tShard size changes:");
				foreach (var zone in equalizer.Zones.OrderBy(_ => _.Main))
				{
					var pressure = zone.Pressure;

					_totalEqualizeReporter.AddPressure(zone.Main, pressure);
					_progressRenderer.WriteLine(
						$"\t\t[{zone.Main}] {zone.InitialSize.ByteSize()} -> {zone.TargetSize.ByteSize()} delta: {zone.Delta.ByteSize()} pressure: {pressure.ByteSize()}");
				}
				_progressRenderer.WriteLine($"\tBound changes:");
				var sb = new StringBuilder();
				sb.Append($"\t\t[{equalizer.Zones.First().Main}]");
				foreach (var zone in equalizer.Zones.Skip(1))
				{
					var bound = zone.Left;
					var shift = zone.Left.RequireShiftSize;
					var targetSymbol = shift == 0 ? "--" : (shift > 0 ? "->" : "<-");
					if (shift < 0)
						shift = -shift;
					sb.Append($" {targetSymbol} {shift.ByteSize()} {targetSymbol} [{bound.RightZone.Main}]");
				}
				_progressRenderer.WriteLine(sb.ToString());

				equalizeWorks.Add(new EqualizeWorkItem(){NS = interval.Namespace, Equalizer = equalizer});
			}

			_progressRenderer.WriteLine();
			_progressRenderer.WriteLine($"\tTotal update pressure:");

			foreach (var pair in _totalEqualizeReporter.TotalPressureByShard)
				_progressRenderer.WriteLine($"\t\t[{pair.Key}] {pair.Value.ByteSize()}");

			return equalizeWorks;
		}

		private class EqualizeWorkItem
		{
			public CollectionNamespace NS { get; set; }
			public ShardSizeEqualizer Equalizer { get; set; }
		}

		private async Task<string> equalizeWork(CollectionNamespace ns, ShardSizeEqualizer equalizer, CancellationToken token)
		{
			if (_totalEqualizeReporter.OutOfLimit)
				return "skipped";

			_commandPlanWriter.Comment($"Equalize shards from {ns}");

			var rounds = 0;
			var progressReporter = new TargetProgressReporter(equalizer.MovedSize, equalizer.RequireMoveSize, LongExtensions.ByteSize);

			while(await equalizer.Equalize())
			{
				rounds++;
				progressReporter.Update(equalizer.MovedSize);
				progressReporter.TryRender(() => new[]
				{
					$"Rounds: {rounds} SizeDeviation: {equalizer.CurrentSizeDeviation.ByteSize()}",
					equalizer.RenderState(),
					_totalEqualizeReporter.RenderState()
				});

				if (_totalEqualizeReporter.OutOfLimit)
					break;

				if(token.IsCancellationRequested)
					break;
			}

			await progressReporter.Stop();

			token.ThrowIfCancellationRequested();

			if (rounds == 0)
			{
				_commandPlanWriter.Comment("no correction");
				_commandPlanWriter.Comment("---");
				_commandPlanWriter.Flush();
				return "no correction";
			}

			foreach (var zone in equalizer.Zones)
			{
				_log.Info("Zone: {0} InitialSize: {1} CurrentSize: {2} TargetSize: {3}",
					zone.Tag, zone.InitialSize.ByteSize(), zone.CurrentSize.ByteSize(), zone.TargetSize.ByteSize());
			}

			_commandPlanWriter.Comment(equalizer.RenderState());
			_commandPlanWriter.Comment("change tags");

			using (var buffer = new TagRangeCommandBuffer(_commandPlanWriter, ns))
			{
				foreach (var tagRange in equalizer.Zones.Select(_ => _.TagRange))
					buffer.RemoveTagRange(tagRange.Min, tagRange.Max, tagRange.Tag);

				foreach (var zone in equalizer.Zones)
					buffer.AddTagRange(zone.Min, zone.Max, zone.Tag);
			}

			_commandPlanWriter.Comment("---");
			_commandPlanWriter.Flush();

			var breakByLimitMessage = _totalEqualizeReporter.OutOfLimit
				? " break by limit"
				: "";

			return $"unmoved data size {equalizer.ElapsedShiftSize.ByteSize()}" + breakByLimitMessage;
		}

		public async Task Run(CancellationToken token)
		{
			_chunkSize = await _clusterSettingsService.GetChunkSize(token);
			var userColls = await _collectionListService.Get(token);
			_collStatsMap = await _collectionStatisticService.Get(userColls, token);
			_shards = await _shardListService.Get(token);
			_shardByTag =  _intervals
				.SelectMany(_ => _.Zones)
				.Distinct()
				.ToDictionary(_ => _, _ => _shards.Single(s => s.Tags.Contains(_)));

			_shardedCollectionInfoByNs = await _shardedCollectionService.Get(token);

			var allTagRangesByNs =  await _tagRangeService.Get(_adjustableIntervals.Select(_ => _.Namespace), token);
			_tagRangesByNs = _adjustableIntervals.ToDictionary(_ => _.Namespace, _ => allTagRangesByNs[_.Namespace].InRange(_.Min, _.Max));

			var allChunksByNs = await _chunkService.Get(_adjustableIntervals.Select(_ => _.Namespace), token);
			_chunksByCollection = _adjustableIntervals.ToDictionary(_ => _.Namespace,
				_ => (IReadOnlyList<ChunkInfo>) allChunksByNs[_.Namespace].FromInterval(_.Min, _.Max));

			createZoneOptimizationDescriptor();
			var equalizeWorks = findSolution(token);

			_progressRenderer.Flush();

			if (!_planOnly)
			{
				var equalizeList = new WorkList();
				foreach (var item in equalizeWorks)
					equalizeList.Add($"From {item.NS.FullName}", new SingleWork(t => equalizeWork(item.NS, item.Equalizer, t)));

				_totalEqualizeReporter.Start();

				await equalizeList.Apply(token); //UNDONE
			}

			_commandPlanWriter.Comment($"\tMoved chunks: {_totalEqualizeReporter.MovedChunks}");
			_commandPlanWriter.Comment($"\tCurrent update pressure:");
			foreach (var (shard, pressure) in _totalEqualizeReporter.CurrentPressureByShard)
				_commandPlanWriter.Comment($"\t\t[{shard}] {pressure.ByteSize()}");
		}

		private class TotalEqualizeReporter
		{
			private readonly long? _moveLimit;
			private Stopwatch _sw;
			private readonly Dictionary<ShardIdentity, long> _totalPressureByShard = new Dictionary<ShardIdentity, long>();

			private long _totalPressure;
			private long _currentPressure;
			private Dictionary<ShardIdentity, long> _limitByShard;
			private Dictionary<ShardIdentity, long> _currentPressureByShard;

			public int MovedChunks { get; private set; }

			public bool OutOfLimit { get; private set; }

			public TotalEqualizeReporter(long? moveLimit)
			{
				_moveLimit = moveLimit;
			}

			public void Start()
			{
				_limitByShard = _totalPressureByShard.ToDictionary(_ => _.Key, _ => Math.Min(_.Value, _moveLimit ?? _.Value));
				_currentPressureByShard = _totalPressureByShard.ToDictionary(_ => _.Key, _ => (long)0);
				_totalPressure = _limitByShard.Sum(_ => _.Value);
				_currentPressure = 0;

				_sw = Stopwatch.StartNew();
			}

			public void ChunkMoving(object sender, ShardSizeEqualizer.ChunkMovingArgs e)
			{
				MovedChunks++;
				_currentPressure += e.ChunkSize;
				_limitByShard[e.Target.Main] -= e.ChunkSize;
				_currentPressureByShard[e.Target.Main] += e.ChunkSize;

				if (_limitByShard[e.Target.Main] < 0)
					OutOfLimit = true;
			}

			public void AddPressure(ShardIdentity shard, long pressure)
			{
				if (_totalPressureByShard.ContainsKey(shard))
					_totalPressureByShard[shard] += pressure;
				else
					_totalPressureByShard[shard] = pressure;
			}

			public IReadOnlyDictionary<ShardIdentity, long> TotalPressureByShard => _totalPressureByShard;

			public IReadOnlyDictionary<ShardIdentity, long> CurrentPressureByShard => _currentPressureByShard;

			public string RenderState()
			{
				if (_totalPressure == 0)
					return "";

				var elapsed = _sw.Elapsed;
				var percent = (double) _currentPressure / _totalPressure;
				var s = percent <= 0 ? 0 : (1 - percent) / percent;

				var eta = TimeSpan.FromSeconds(elapsed.TotalSeconds * s);

				return $"All progress {_currentPressure.ByteSize()}/{_totalPressure.ByteSize()} Moved chunks: {MovedChunks} Elapsed: {elapsed:d\\.hh\\:mm\\:ss\\.f} ETA: {eta:d\\.hh\\:mm\\:ss\\.f}";
			}
		}
	}
}
