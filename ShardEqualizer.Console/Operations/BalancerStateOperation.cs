using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using ShardEqualizer.Models;

namespace ShardEqualizer.Operations
{
	public class BalancerStateOperation: IOperation
	{
		private readonly ShardListService _shardListService;
		private readonly TagRangeService _tagRangeService;
		private readonly ChunkRepository _chunkRepo;
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly ProgressRenderer _progressRenderer;

		public BalancerStateOperation(
			ShardListService shardListService,
			TagRangeService tagRangeService,
			ChunkRepository chunkRepo,
			IReadOnlyList<Interval> intervals,
			ProgressRenderer progressRenderer)
		{
			_shardListService = shardListService;
			_tagRangeService = tagRangeService;
			_chunkRepo = chunkRepo;
			_intervals = intervals;
			_progressRenderer = progressRenderer;
		}

		private async Task<IList<UnMovedChunk>> scanInterval(Interval interval, IReadOnlyCollection<Shard> shards,
			IReadOnlyDictionary<CollectionNamespace, IReadOnlyList<TagRange>> tagRangesByNs, ProgressReporter reporter,
			CancellationToken token)
		{
			var currentTags = new HashSet<TagIdentity>(interval.Zones);
			var tagRanges = tagRangesByNs[interval.Namespace];
			tagRanges = tagRanges.Where(_ => currentTags.Contains(_.Tag)).ToList();

			var result = new List<UnMovedChunk>();

			foreach (var tagRange in tagRanges)
			{
				var validShards = shards.Where(_ => _.Tags.Contains(tagRange.Tag)).Select(_ => _.Id).ToList();

				var unMovedChunks = await (await _chunkRepo.ByNamespace(interval.Namespace)
						.From(tagRange.Min).To(tagRange.Max).NoJumbo().ExcludeShards(validShards).Find(token))
					.ToListAsync(token);

				if (unMovedChunks.Count == 0) continue;

				result.Add(new UnMovedChunk()
				{
					Namespace = interval.Namespace,
					TagRange = tagRange.Tag,
					Count = unMovedChunks.Count,
					SourceShards = unMovedChunks.Select(_ => _.Shard).Distinct().Select(_ => $"'{_}'").ToList(),
				});
			}

			reporter.Increment();
			return result;
		}

		private async Task<IList<UnMovedChunk>> scanIntervals(CancellationToken token)
		{
			var shards = await _shardListService.Get(token);
			var tagRangesByNs = await _tagRangeService.Get(_intervals.Select(_ => _.Namespace), token);

			await using var reporter = _progressRenderer.Start("Scan intervals", _intervals.Count);

			var unMovedChunksList = await _intervals.ParallelsAsync((interval, t) => scanInterval(interval, shards, tagRangesByNs, reporter, t), 32, token);

			var result = unMovedChunksList.SelectMany(_ => _).ToList();

			var totalUnMovedChunks = result.Sum(_ => _.Count);

			reporter.SetCompleteMessage(totalUnMovedChunks == 0
				? "all chunks moved."
				: $"found {totalUnMovedChunks} chunks is awaiting movement.");

			return result;
		}

		public async Task Run(CancellationToken token)
		{
			var unMovedChunks = await scanIntervals(token);

			foreach (var unMovedChunkGroup in unMovedChunks.GroupBy(_ => _.Namespace).OrderBy(_ => _.Key.FullName))
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
