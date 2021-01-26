using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using ShardEqualizer.Models;
using ShardEqualizer.MongoCommands;

namespace ShardEqualizer.Operations
{
	public class MergeChunksOperation : IOperation
	{
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly ProgressRenderer _progressRenderer;
		private readonly ShardListService _shardListService;
		private readonly TagRangeService _tagRangeService;
		private readonly ChunkRepository _chunkRepo;
		private readonly CommandPlanWriter _commandPlanWriter;

		public MergeChunksOperation(
			ShardListService shardListService,
			TagRangeService tagRangeService,
			ChunkRepository chunkRepo, //UNDONE use ChunkService
			IReadOnlyList<Interval> intervals,
			ProgressRenderer progressRenderer,
			CommandPlanWriter commandPlanWriter)
		{
			_shardListService = shardListService;
			_tagRangeService = tagRangeService;
			_chunkRepo = chunkRepo;
			_commandPlanWriter = commandPlanWriter;

			if (intervals.Count == 0)
				throw new ArgumentException("interval list is empty");

			_intervals = intervals;
			_progressRenderer = progressRenderer;
		}

		private async Task<Tuple<List<MergeCommand>, int>> mergeInterval(IEnumerable<Shard> shards, MergeZone zone, ProgressReporter progressReporter,
			CancellationToken token)
		{
			var mergeCommands = new List<MergeCommand>();
			var mergedChunks = 0;
			var validShards = shards.Where(_ => _.Tags.Contains(zone.TagRange.Tag)).Select(_ => _.Id).ToList();

			var mergeCandidates = await (await _chunkRepo.ByNamespace(zone.Interval.Namespace)
				.From(zone.TagRange.Min).To(zone.TagRange.Max).NoJumbo().ByShards(validShards).Find(token))
				.ToListAsync(token);

			foreach (var shardGroup in mergeCandidates.GroupBy(_ => _.Shard))
				mergedChunks += mergeInterval(zone.Interval.Namespace, shardGroup.ToList(), mergeCommands);

			progressReporter.Increment();

			return new Tuple<List<MergeCommand>, int>(mergeCommands, mergedChunks);
		}

		private static int mergeInterval(CollectionNamespace ns, IList<Chunk> chunks, ICollection<MergeCommand> mergeCommands)
		{
			var mergedChunks = 0;
			if (chunks.Count <= 1)
				return mergedChunks;

			chunks = chunks.OrderBy(_ => _.Min).ToList();

			var left = chunks.First();
			var minBound = left.Min;
			var merged = 0;

			foreach (var chunk in chunks.Skip(1))
			{
				if (left.Max == chunk.Min)
				{
					left = chunk;
					mergedChunks++;
					merged++;
					continue;
				}

				if(merged > 1)
					mergeCommands.Add(new MergeCommand(ns, left.Shard, minBound, left.Max));

				left = chunk;
				minBound = left.Min;
				merged = 0;
			}

			if(merged > 1)
				mergeCommands.Add(new MergeCommand(ns, left.Shard, minBound, left.Max));

			return mergedChunks;
		}

		private async Task<List<MergeCommand>> mergeIntervals(IReadOnlyCollection<Shard> shards,
			IReadOnlyCollection<MergeZone> mergeZones, CancellationToken token)
		{
			var allMergeCommands = new List<MergeCommand>();

			await using var reporter = _progressRenderer.Start($"Merge intervals", mergeZones.Count);
			{
				var results = await mergeZones.ParallelsAsync((zone, t) => mergeInterval(shards, zone, reporter, t), 16, token);

				var allMergedChunks = 0;
				foreach (var (mergeCommands, mergedChunks) in results)
				{
					allMergeCommands.AddRange(mergeCommands);
					allMergedChunks += mergedChunks;
				}

				reporter.SetCompleteMessage(allMergedChunks == 0
					? "No chunks to merge."
					: $"Merged {allMergedChunks} chunks.");
			}

			return allMergeCommands;
		}

		private void writeCommandFile(IEnumerable<MergeCommand> mergeCommands)
		{
			foreach (var nsGroup in mergeCommands.GroupBy(_ => _.Ns).OrderBy(_ => _.Key.FullName))
			{
				_commandPlanWriter.Comment($"merge chunks on {nsGroup.Key}");

				foreach (var shardGroup in nsGroup.GroupBy(_ =>_.Shard).OrderBy(_ => _.Key))
				{
					_commandPlanWriter.Comment($"  shard: {shardGroup.Key}");

					foreach (var mergeCommand in shardGroup.OrderBy(_ => _.Min))
						_commandPlanWriter.MergeChunks(mergeCommand.Ns, mergeCommand.Min, mergeCommand.Max);
				}

				_commandPlanWriter.Comment(" --");
			}
		}

		public async Task Run(CancellationToken token)
		{
			var shards = await _shardListService.Get(token);
			var tagRangesByNs = await _tagRangeService.Get(_intervals.Select(_ => _.Namespace), token);

			var mergeZones = _intervals
				.SelectMany(interval => tagRangesByNs[interval.Namespace].Select(tagRange => new {interval, tagRange}))
				.Select(_ => new MergeZone(_.interval, _.tagRange))
				.ToList();

			var mergeCommands = await mergeIntervals(shards, mergeZones, token);

			writeCommandFile(mergeCommands);
		}

		private class MergeZone
		{
			public MergeZone(Interval interval, TagRange tagRange)
			{
				Interval = interval;
				TagRange = tagRange;
			}

			public Interval Interval { get; }
			public TagRange TagRange { get; }
		}

		private class MergeCommand
		{
			public CollectionNamespace Ns { get; }
			public ShardIdentity Shard { get; }
			public BsonBound Min { get; }
			public BsonBound Max { get; }

			public MergeCommand(CollectionNamespace ns, ShardIdentity shard, BsonBound min, BsonBound max)
			{
				Ns = ns;
				Shard = shard;
				Min = min;
				Max = max;
			}
		}
	}
}
