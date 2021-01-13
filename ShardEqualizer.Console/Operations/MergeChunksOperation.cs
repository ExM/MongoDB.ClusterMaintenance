using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using NLog;
using ShardEqualizer.Models;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.WorkFlow;

namespace ShardEqualizer.Operations
{
	public class MergeChunksOperation : IOperation
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();

		private readonly IReadOnlyList<Interval> _intervals;
		private readonly IConfigDbRepositoryProvider _configDb;
		private readonly IMongoClient _mongoClient;
		private readonly CommandPlanWriter _commandPlanWriter;
		private int _mergedChunks;
		private IReadOnlyCollection<Shard> _shards;
		private IReadOnlyList<MergeZone> _mergeZones;

		private ConcurrentBag<MergeCommand> _mergeCommands = new ConcurrentBag<MergeCommand>();

		public MergeChunksOperation(IConfigDbRepositoryProvider configDb, IReadOnlyList<Interval> intervals, IMongoClient mongoClient, CommandPlanWriter commandPlanWriter)
		{
			_configDb = configDb;
			_mongoClient = mongoClient;
			_commandPlanWriter = commandPlanWriter;

			if (intervals.Count == 0)
				throw new ArgumentException("interval list is empty");

			_intervals = intervals;
		}

		private async Task<string> loadShards(CancellationToken token)
		{
			_shards = await _configDb.Shards.GetAll();
			return $"found {_shards.Count} shards.";
		}

		private async Task<(Interval interval, IList<TagRange> tagRanges)> loadTag(Interval interval, CancellationToken token)
		{
			var currentTags = new HashSet<TagIdentity>(interval.Zones);

			var tagRanges = (await _configDb.Tags.Get(interval.Namespace))
				.Where(_ => currentTags.Contains(_.Tag))
				.ToList();

			return (interval, tagRanges);
		}

		private ObservableTask loadTags(CancellationToken token)
		{
			return ObservableTask.WithParallels(
				_intervals,
				16,
				loadTag,
				loadedTags =>
				{
					_mergeZones = loadedTags.SelectMany(_ => _.tagRanges.Select(tagRange => new MergeZone(_.interval, tagRange))).ToList();
				},
				token);
		}

		private async Task mergeInterval(MergeZone zone, CancellationToken token)
		{
			var validShards = _shards.Where(_ => _.Tags.Contains(zone.TagRange.Tag)).Select(_ => _.Id).ToList();

			var mergeCandidates = await (await _configDb.Chunks.ByNamespace(zone.Interval.Namespace)
				.From(zone.TagRange.Min).To(zone.TagRange.Max).NoJumbo().ByShards(validShards).Find())
				.ToListAsync(token);

			foreach (var shardGroup in mergeCandidates.GroupBy(_ => _.Shard))
				mergeInterval(zone.Interval.Namespace, shardGroup.ToList());
		}

		private void mergeInterval(CollectionNamespace ns, IList<Chunk> chunks)
		{
			if (chunks.Count <= 1)
				return;

			chunks = chunks.OrderBy(_ => _.Min).ToList();

			var left = chunks.First();
			var minBound = left.Min;
			var merged = 0;

			foreach (var chunk in chunks.Skip(1))
			{
				if (left.Max == chunk.Min)
				{
					left = chunk;
					Interlocked.Increment(ref _mergedChunks);
					merged++;
					continue;
				}

				if(merged > 1)
					_mergeCommands.Add(new MergeCommand(ns, left.Shard, minBound, left.Max));

				left = chunk;
				minBound = left.Min;
				merged = 0;
			}

			if(merged > 1)
				_mergeCommands.Add(new MergeCommand(ns, left.Shard, minBound, left.Max));
		}

		private ObservableTask mergeIntervals(CancellationToken token)
		{
			return ObservableTask.WithParallels(
				_mergeZones,
				16,
				mergeInterval,
				token);
		}

		private void writeCommandFile(CancellationToken token)
		{
			foreach (var nsGroup in _mergeCommands.GroupBy(_ => _.Ns).OrderBy(_ => _.Key.FullName))
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
			var opList = new WorkList()
			{
				{ "Load shard list", new SingleWork(loadShards)},
				{ "Load tags", new ObservableWork(loadTags, () => $"found {_mergeZones.Count} tag ranges.")},
				{ "Merge intervals", new ObservableWork(mergeIntervals, () => _mergedChunks == 0
					? "No chunks to merge."
					: $"Merged {_mergedChunks} chunks.")},
				{"Write command file", writeCommandFile}
			};

			await opList.Apply(token);
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
