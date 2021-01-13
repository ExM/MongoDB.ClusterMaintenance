using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using NLog;
using ShardEqualizer.Models;
using ShardEqualizer.WorkFlow;

namespace ShardEqualizer.Operations
{
	public class BalancerStateOperation: IOperation
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();

		private readonly IConfigDbRepositoryProvider _configDb;
		private readonly IReadOnlyList<Interval> _intervals;

		private IReadOnlyCollection<Shard> _shards;
		private int _totalUnMovedChunks = 0;
		private readonly ConcurrentBag<UnMovedChunk> _unMovedChunks = new ConcurrentBag<UnMovedChunk>();

		public BalancerStateOperation(IConfigDbRepositoryProvider configDb, IReadOnlyList<Interval> intervals)
		{
			_intervals = intervals;
			_configDb = configDb;
		}

		private async Task<string> loadShards(CancellationToken token)
		{
			_shards = await _configDb.Shards.GetAll();
			return $"found {_shards.Count} shards.";
		}

		private async Task<int> scanInterval(Interval interval, CancellationToken token)
		{
			var currentTags = new HashSet<TagIdentity>(interval.Zones);
			var tagRanges = await _configDb.Tags.Get(interval.Namespace);
			tagRanges = tagRanges.Where(_ => currentTags.Contains(_.Tag)).ToList();

			var intervalCount = 0;

			foreach (var tagRange in tagRanges)
			{
				var validShards = _shards.Where(_ => _.Tags.Contains(tagRange.Tag)).Select(_ => _.Id).ToList();

				var unMovedChunks = await (await _configDb.Chunks.ByNamespace(interval.Namespace)
						.From(tagRange.Min).To(tagRange.Max).NoJumbo().ExcludeShards(validShards).Find())
					.ToListAsync(token);

				if (unMovedChunks.Count == 0) continue;

				_unMovedChunks.Add(new UnMovedChunk()
				{
					Namespace = interval.Namespace,
					TagRange = tagRange.Tag,
					Count = unMovedChunks.Count,
					SourceShards = unMovedChunks.Select(_ => _.Shard).Distinct().Select(_ => $"'{_}'").ToList(),
				});

				intervalCount += unMovedChunks.Count;
			}

			return intervalCount;
		}

		private ObservableTask scanIntervals(CancellationToken token)
		{
			return ObservableTask.WithParallels(
				_intervals.ToList(),
				16,
				scanInterval,
				intervalCounts => { _totalUnMovedChunks = intervalCounts.Sum(); },
				token);
		}

		public async Task Run(CancellationToken token)
		{
			var opList = new WorkList()
			{
				{ "Load shard list", new SingleWork(loadShards)},
				{ "Scan intervals", new ObservableWork(scanIntervals, () => _totalUnMovedChunks == 0
					? "all chunks moved."
					: $"found {_totalUnMovedChunks} chunks is awaiting movement.")}
			};

			await opList.Apply(token);

			foreach (var unMovedChunkGroup in _unMovedChunks.GroupBy(_ => _.Namespace).OrderBy(_ => _.Key.FullName))
			{
				Console.WriteLine("{0}:", unMovedChunkGroup.Key);
				foreach (var  unMovedChunk in unMovedChunkGroup.OrderBy(_ => _.TagRange))
				{
					Console.WriteLine("  tag range '{0}' wait {1} chunks from {2} shards",
						unMovedChunk.TagRange, unMovedChunk.Count, string.Join(", ", unMovedChunk.SourceShards));
				}
			}
		}

		private class UnMovedChunk
		{
			public CollectionNamespace Namespace { get; set; }
			public TagIdentity TagRange { get; set; }
			public int Count { get; set; }
			public List<string> SourceShards { get; set; }
		}
	}
}
