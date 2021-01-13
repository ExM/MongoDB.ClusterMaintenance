using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using NLog;
using ShardEqualizer.Config;
using ShardEqualizer.Models;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.WorkFlow;

namespace ShardEqualizer.Operations
{
	public class PresplitDataOperation : IOperation
	{
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly CommandPlanWriter _commandPlanWriter;
		private readonly bool _renew;
		private readonly IConfigDbRepositoryProvider _configDb;

		private static readonly Logger _log = LogManager.GetCurrentClassLogger();

		public PresplitDataOperation(IConfigDbRepositoryProvider configDb, IReadOnlyList<Interval> intervals, CommandPlanWriter commandPlanWriter, bool renew)
		{
			_configDb = configDb;
			_intervals = intervals;
			_commandPlanWriter = commandPlanWriter;
			_renew = renew;

			if (intervals.Count == 0)
				throw new ArgumentException("interval list is empty");

			_intervals = intervals;
		}

		private async Task createPresplitCommandForInterval(Interval interval, CancellationToken token)
		{

			var preSplit = interval.PreSplit;

			if (preSplit == PreSplitMode.Auto)
			{
				if (interval.Min.HasValue && interval.Max.HasValue)
				{
					var totalChunks = await _configDb.Chunks
						.ByNamespace(interval.Namespace)
						.From(interval.Min)
						.To(interval.Max).Count();

					preSplit = totalChunks / interval.Zones.Count < 100
						? PreSplitMode.Interval
						: PreSplitMode.Chunks;

					_log.Info("detect presplit mode of {0} with total chunks {1}", interval.Namespace.FullName, totalChunks);
				}
				else
				{
					preSplit = PreSplitMode.Chunks;

					_log.Info("detect presplit mode of {0} without bounds", interval.Namespace.FullName);
				}
			}

			using (var buffer = new TagRangeCommandBuffer(_commandPlanWriter, interval.Namespace))
			{
				var comments = new List<string>();
				comments.Add($"presplit commands for {interval.Namespace.FullName}");

				if (await removeOldTagRangesIfRequired(interval, buffer))
				{
					_log.Info("presplit data of {0} with mode {1}", interval.Namespace.FullName, preSplit);

					switch (preSplit)
					{
						case PreSplitMode.Interval:
							await presplitData(interval, buffer, token);
							break;
						case PreSplitMode.Chunks:
							await distributeCollection(interval, buffer, token);
							break;

						case PreSplitMode.Auto:
						default:
							throw new NotSupportedException($"unexpected PreSplitType:{preSplit}");
					}
				}
				else
				{
					comments.Add($"zones not changed");
				}

				lock (_commandPlanWriter)
				{
					foreach (var comment in comments)
						_commandPlanWriter.Comment(comment);
					buffer.Flush();
					_commandPlanWriter.Comment($"---");
				}
			}
		}

		private ObservableTask createPresplitCommands(CancellationToken token)
		{
			return ObservableTask.WithParallels(
				_intervals,
				16,
				createPresplitCommandForInterval,
				token);
		}

		public async Task Run(CancellationToken token)
		{
			var opList = new WorkList()
			{
				{ "Create presplit commands", new ObservableWork(createPresplitCommands)},
			};

			await opList.Apply(token);
		}

		private async Task<bool> removeOldTagRangesIfRequired(Interval interval, TagRangeCommandBuffer buffer)
		{
			var tagRanges = await _configDb.Tags.Get(interval.Namespace, interval.Min, interval.Max);

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

		private async Task presplitData(Interval interval, TagRangeCommandBuffer buffer, CancellationToken token)
		{
			var collInfo = await _configDb.Collections.Find(interval.Namespace);

			if (collInfo == null)
				throw new InvalidOperationException($"collection {interval.Namespace.FullName} not sharded");

			if(interval.Min == null || interval.Max == null)
				throw new InvalidOperationException($"collection {interval.Namespace.FullName} - bounds not found in configuration");

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

		private async Task distributeCollection(Interval interval, TagRangeCommandBuffer buffer,
			CancellationToken token)
		{
			var collInfo = await _configDb.Collections.Find(interval.Namespace);

			if (collInfo == null)
				throw new InvalidOperationException($"collection {interval.Namespace.FullName} not sharded");

			var filtered = _configDb.Chunks
				.ByNamespace(interval.Namespace)
				.From(interval.Min)
				.To(interval.Max);

			var chunks = await (await filtered.Find()).ToListAsync(token);

			if(chunks.Count < interval.Zones.Count)
				throw new InvalidOperationException($"collection {interval.Namespace.FullName} does not contain enough chunks");

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
