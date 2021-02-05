using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using NLog;
using ShardEqualizer.ChunkCaching;
using ShardEqualizer.Models;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer.Operations
{
	public class PresplitDataOperation : IOperation
	{
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly ProgressRenderer _progressRenderer;
		private readonly CommandPlanWriter _commandPlanWriter;
		private readonly bool _renew;
		private readonly ShardedCollectionService _shardedCollectionService;
		private readonly TagRangeService _tagRangeService;
		private readonly ChunkService _chunkService;

		private static readonly Logger _log = LogManager.GetCurrentClassLogger();
		private IReadOnlyDictionary<CollectionNamespace, ShardedCollectionInfo> _shardedCollectionByNs;
		private IReadOnlyDictionary<CollectionNamespace, IReadOnlyList<TagRange>> _tagRangesByNs;
		private IReadOnlyDictionary<CollectionNamespace, ChunksCache> _allChunksByNs;

		public PresplitDataOperation(
			ShardedCollectionService shardedCollectionService,
			TagRangeService tagRangeService,
			ChunkService chunkService,
			IReadOnlyList<Interval> intervals,
			ProgressRenderer progressRenderer,
			CommandPlanWriter commandPlanWriter,
			bool renew)
		{
			_shardedCollectionService = shardedCollectionService;
			_tagRangeService = tagRangeService;
			_chunkService = chunkService;
			_intervals = intervals;
			_progressRenderer = progressRenderer;
			_commandPlanWriter = commandPlanWriter;
			_renew = renew;

			if (intervals.Count == 0)
				throw new ArgumentException("interval list is empty");

			_intervals = intervals;
		}
		public async Task Run(CancellationToken token)
		{
			_shardedCollectionByNs = await _shardedCollectionService.Get(token);
			_tagRangesByNs =  await _tagRangeService.Get(_intervals.Select(_ => _.Namespace), token);
			_allChunksByNs = await _chunkService.Get(_intervals.Select(_ => _.Namespace), token);

			_progressRenderer.WriteLine("Create presplit commands");

			foreach (var interval in _intervals)
				createPresplitCommandForInterval(interval);
		}

		private void createPresplitCommandForInterval(Interval interval)
		{
			using (var buffer = new TagRangeCommandBuffer(_commandPlanWriter, interval.Namespace))
			{
				_commandPlanWriter.Comment($"presplit commands for {interval.Namespace.FullName}");

				if (removeOldTagRangesIfRequired(interval, buffer))
					presplitInterval(interval, buffer);
				else
					_commandPlanWriter.Comment($"zones not changed");
			}

			_commandPlanWriter.Comment($"---");
		}

		private bool removeOldTagRangesIfRequired(Interval interval, TagRangeCommandBuffer buffer)
		{
			var tagRanges = _tagRangesByNs[interval.Namespace].InRange(interval.Min, interval.Max);

			if (!_renew && tagRanges.Select(_ => _.Tag).SequenceEqual(interval.Zones))
			{
				if (!interval.Min.HasValue || !interval.Max.HasValue)
					return false;

				if (tagRanges.First().Min == interval.Min.Value &&
				    tagRanges.Last().Max == interval.Max.Value)
					return false;
			}

			foreach (var tagRange in tagRanges)
				buffer.RemoveTagRange(tagRange.Min, tagRange.Max, tagRange.Tag);

			return true;
		}

		private void presplitInterval(Interval interval, TagRangeCommandBuffer buffer)
		{
			var collInfo = _shardedCollectionByNs[interval.Namespace];

			if (collInfo == null || collInfo.Dropped)
				throw new InvalidOperationException($"collection {interval.Namespace.FullName} not sharded");

			if (!interval.Adjustable)
			{
				if(interval.Min == null || interval.Max == null)
					throw new InvalidOperationException($"collection {interval.Namespace.FullName} - bounds not found in configuration");

				presplitByBounds(interval, buffer);
				return;
			}

			var chunks = _allChunksByNs[interval.Namespace].FromInterval(interval.Min, interval.Max);

			if (chunks.Length < interval.Zones.Count)
			{
				if(interval.Min == null || interval.Max == null)
					throw new InvalidOperationException($"collection {interval.Namespace.FullName} does not contain enough chunks " +
					                                    $"or specify the boundaries of the interval in the configuration");

				presplitByBounds(interval, buffer);
				return;
			}

			presplitByChunks(interval, chunks, buffer);
		}

		private void presplitByBounds(Interval interval, TagRangeCommandBuffer buffer)
		{
			var internalBounds = BsonSplitter.SplitFirstValue(interval.Min.Value, interval.Max.Value, interval.Zones.Count)
				.ToList();

			var allBounds = new List<BsonBound>(internalBounds.Count + 2);

			allBounds.Add(interval.Min.Value);
			allBounds.AddRange(internalBounds);
			allBounds.Add(interval.Max.Value);

			var zoneIndex = 0;
			foreach (var range in allBounds.Zip(allBounds.Skip(1), (min, max) => new {min, max}))
			{
				var zoneName = interval.Zones[zoneIndex];
				zoneIndex++;

				buffer.AddTagRange(range.min, range.max, zoneName);
			}
		}

		private void presplitByChunks(Interval interval, IList<ChunkInfo> chunks, TagRangeCommandBuffer buffer)
		{
			var parts = chunks.Split(interval.Zones.Count).Select((items, order) => new {Items = items, Order = order}).ToList();
			foreach (var part in parts)
			{
				var zoneName = interval.Zones[part.Order];

				var minBound = part.Items.First().Min;
				var maxBound = part.Items.Last().Max;

				if (part.Order == 0 && interval.Min.HasValue)
					minBound = interval.Min.Value;

				if (part.Order == interval.Zones.Count - 1 && interval.Max.HasValue)
					maxBound = interval.Max.Value;

				buffer.AddTagRange(minBound, maxBound, zoneName);
			}
		}
	}
}
