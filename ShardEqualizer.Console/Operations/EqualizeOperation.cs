using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using NLog;
using ShardEqualizer.Models;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.Reporting;
using ShardEqualizer.ShardSizeEqualizing;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer.Operations
{
	public class EqualizeOperation: IOperation
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();

		private readonly IReadOnlyList<Interval> _intervals;
		private readonly ShardListService _shardListService;
		private readonly CollectionListService _collectionListService;
		private readonly CollectionStatisticService _collectionStatisticService;
		private readonly TagRangeService _tagRangeService;
		private readonly ClusterSettingsService _clusterSettingsService;
		private readonly ChunkService _chunkService;
		private readonly ChunkSizeService _chunkSizeService;
		private readonly ProgressRenderer _progressRenderer;
		private readonly CommandPlanWriter _commandPlanWriter;
		private readonly long? _moveLimit;
		private readonly bool _planOnly;

		public EqualizeOperation(
			ShardListService shardListService,
			CollectionListService collectionListService,
			CollectionStatisticService collectionStatisticService,
			TagRangeService tagRangeService,
			ClusterSettingsService clusterSettingsService,
			ChunkService chunkService,
			ChunkSizeService chunkSizeService,
			IReadOnlyList<Interval> intervals,
			ProgressRenderer progressRenderer,
			CommandPlanWriter commandPlanWriter,
			long? moveLimit,
			bool planOnly)
		{
			_shardListService = shardListService;
			_collectionListService = collectionListService;
			_collectionStatisticService = collectionStatisticService;
			_tagRangeService = tagRangeService;
			_clusterSettingsService = clusterSettingsService;
			_chunkService = chunkService;
			_chunkSizeService = chunkSizeService;
			_progressRenderer = progressRenderer;
			_commandPlanWriter = commandPlanWriter;
			_moveLimit = moveLimit;
			_planOnly = planOnly;

			if (intervals.Count == 0)
				throw new ArgumentException("interval list is empty");

			_intervals = intervals;
			_adjustableIntervals = _intervals.Where(_ => _.Adjustable).ToList();
		}

		private IReadOnlyCollection<Shard> _shards;
		private long _chunkSize;
		private IReadOnlyDictionary<CollectionNamespace, CollectionStatistics> _collStatsMap;
		private IReadOnlyDictionary<CollectionNamespace, IReadOnlyList<ChunkInfo>> _chunksByCollection;
		private ZoneOptimizationDescriptor _zoneOpt;
		private Dictionary<TagIdentity, Shard> _shardByTag;
		private IReadOnlyDictionary<CollectionNamespace, IReadOnlyList<TagRange>> _tagRangesByNs;
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
				collCfg.Adjustable = interval.Adjustable;
				collCfg.Priority = 1;

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
			return new ChunkCollection(_chunksByCollection[ns], chunk => _chunkSizeService.Get(ns, chunk.Min, chunk.Max, token));
		}

		private List<EqualizeWorkItem> findSolution(CancellationToken token)
		{
			_progressRenderer.WriteLine($"Find solution");

			//File.WriteAllText("conditionDump.js", _zoneOpt.Serialize());

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

				equalizeWorks.Add(new EqualizeWorkItem(interval.Namespace, equalizer));
			}

			return equalizeWorks;
		}

		private void renderShardSizeChanges(EqualizeWorkItem item)
		{
			var equalizer = item.Equalizer;
			var scaleSuffix = equalizer.Zones.Max(_ => _.TargetSize).OptimalScaleSuffix();

			_progressRenderer.WriteLine($"  {item.Ns}");
			_progressRenderer.WriteLine($"    Require move: {equalizer.RequireMoveSize.ByteSize()}");
			_progressRenderer.WriteLine($"    Equalize details (in {scaleSuffix.Text()}b)");
			foreach (var line in equalizer.RenderMovePlan(scaleSuffix))
			{
				_progressRenderer.WriteLine("    " + line);
			}
		}

		private class EqualizeWorkItem
		{
			public CollectionNamespace Ns { get; }
			public ShardSizeEqualizer Equalizer { get; }

			public EqualizeWorkItem(CollectionNamespace ns, ShardSizeEqualizer equalizer)
			{
				Ns = ns;
				Equalizer = equalizer;
			}

			public string[] RenderCompleteState()
			{
				return new[]
				{
					$"Equalize {Ns} completed (unmoved data size {Equalizer.ElapsedShiftSize.ByteSize()})",
					Equalizer.RenderState(),
				};
			}

			public void RenderCommandPlan(CommandPlanWriter commandPlanWriter)
			{
				commandPlanWriter.Comment($"Equalize shards from {Ns}");

				if (Equalizer.MovedSize == 0)
				{
					commandPlanWriter.Comment("no correction");
					commandPlanWriter.Comment("---");
					commandPlanWriter.Flush();
					return;
				}

				foreach (var zone in Equalizer.Zones)
				{
					_log.Info("Zone: {0} InitialSize: {1} CurrentSize: {2} TargetSize: {3}",
						zone.Tag, zone.InitialSize.ByteSize(), zone.CurrentSize.ByteSize(), zone.TargetSize.ByteSize());
				}

				commandPlanWriter.Comment(Equalizer.RenderState());
				commandPlanWriter.Comment("change tags");

				using (var buffer = new TagRangeCommandBuffer(commandPlanWriter, Ns))
				{
					foreach (var tagRange in Equalizer.Zones.Select(_ => _.TagRange))
						buffer.RemoveTagRange(tagRange.Min, tagRange.Max, tagRange.Tag);

					foreach (var zone in Equalizer.Zones)
						buffer.AddTagRange(zone.Min, zone.Max, zone.Tag);
				}

				commandPlanWriter.Comment("---");
				commandPlanWriter.Flush();
			}

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

			var allTagRangesByNs =  await _tagRangeService.Get(_adjustableIntervals.Select(_ => _.Namespace), token);
			_tagRangesByNs = _adjustableIntervals.ToDictionary(_ => _.Namespace, _ => allTagRangesByNs[_.Namespace].InRange(_.Min, _.Max));

			var allChunksByNs = await _chunkService.Get(_adjustableIntervals.Select(_ => _.Namespace), token);
			_chunksByCollection = _adjustableIntervals.ToDictionary(_ => _.Namespace,
				_ => (IReadOnlyList<ChunkInfo>) allChunksByNs[_.Namespace].FromInterval(_.Min, _.Max));

			createZoneOptimizationDescriptor();
			var equalizeWorks = findSolution(token);

			_progressRenderer.WriteLine("Data move plan:");
			foreach (var item in equalizeWorks)
			{
				renderShardSizeChanges(item);
				_progressRenderer.WriteLine();
			}

			_progressRenderer.WriteLine($"Total update pressure:");
			foreach (var pair in equalizeWorks.SelectMany(_ => _.Equalizer.Zones).GroupBy(_ => _.Main).OrderBy(_ => _.Key))
				_progressRenderer.WriteLine($"  [{pair.Key}] {pair.Sum(_ => _.RequirePressure).ByteSize()}");
			_progressRenderer.WriteLine();

			if (!_planOnly)
			{
				var updateQuotes = _shards.ToDictionary(_ => _.Id, _ => _moveLimit);

				foreach (var item in equalizeWorks.OrderByDescending(_ => _.Equalizer.RequireMoveSize).ToList())
				{
					item.Equalizer.SetQuotes(updateQuotes);
				}

				equalizeWorks = equalizeWorks.Where(_ => _.Equalizer.RequireMoveSize > 0).ToList();

				_progressRenderer.WriteLine("Quoted plan:");
				foreach (var item in equalizeWorks)
				{
					renderShardSizeChanges(item);
					_progressRenderer.WriteLine();
				}

				var movedChunks = 0;

				try
				{
					movedChunks = await runEqualizeAllCollections(equalizeWorks, token);
				}
				finally
				{
					foreach (var item in equalizeWorks)
						item.RenderCommandPlan(_commandPlanWriter);

					_commandPlanWriter.Comment($"\tMoved chunks: {movedChunks}");
					_commandPlanWriter.Comment($"\tCurrent update pressure:");

					foreach (var shard in equalizeWorks.SelectMany(_ => _.Equalizer.Zones).GroupBy(_ => _.Main).OrderBy(_ => _.Key))
						_commandPlanWriter.Comment($"\t\t[{shard.Key}] {shard.Sum(_ => _.CurrentPressure).ByteSize()}");
				}
			}
		}

		private async Task<int> runEqualizeAllCollections(IList<EqualizeWorkItem> equalizeWorkItems, CancellationToken token)
		{
			var movedChunkCount = 0;
			var totalPressure = equalizeWorkItems.Sum(_ => _.Equalizer.RequireMoveSize);

			await using var reporter = _progressRenderer.Start($"Equalize all collections", totalPressure, LongExtensions.ByteSize);
			{
				foreach (var item in equalizeWorkItems)
				{
					while (!token.IsCancellationRequested)
					{
						_log.Debug("Equalize {0}", item.Ns);

						var moved = await item.Equalizer.Equalize(); // UNDONE use token

						if (!moved.IsSuccess)
							break;

						movedChunkCount++;
						reporter.Increment(moved.MovedChunkSize);
					}

					foreach (var line in item.RenderCompleteState())
						_progressRenderer.WriteLine(line);
				}

				reporter.SetCompleteMessage($"moved {movedChunkCount} chunks");
			}

			return movedChunkCount;
		}
	}
}
