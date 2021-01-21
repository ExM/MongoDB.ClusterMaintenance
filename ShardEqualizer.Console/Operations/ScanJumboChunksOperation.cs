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

namespace ShardEqualizer.Operations
{
	public class ScanJumboChunksOperation : IOperation
	{
		private readonly IMongoClient _mongoClient;
		private readonly ProgressRenderer _progressRenderer;
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly ShardedCollectionService _shardedCollectionService;
		private readonly ChunkRepository _chunkRepo;
		private List<Chunk> _jumboChunks;
		private ConcurrentBag<ChunkDataSize> _chunkDataSizes = new ConcurrentBag<ChunkDataSize>();
		private int _dataSizeCommandErrors = 0;

		public ScanJumboChunksOperation(
			IReadOnlyList<Interval> intervals,
			ShardedCollectionService shardedCollectionService,
			ChunkRepository chunkRepo,
			IMongoClient mongoClient,
			ProgressRenderer progressRenderer)
		{
			_mongoClient = mongoClient;
			_progressRenderer = progressRenderer;

			if (intervals.Count == 0)
				throw new ArgumentException("interval list is empty");

			_intervals = intervals;
			_shardedCollectionService = shardedCollectionService;
			_chunkRepo = chunkRepo;
		}

		private async Task findJumboChunks(CancellationToken token)
		{
			await using var reporter = _progressRenderer.Start($"Find jumbo chunks", _intervals.Count);
			{
				async Task<List<Chunk>> loadCollChunks(Interval interval, CancellationToken t)
				{
					var allChunks = await (await _chunkRepo
						.ByNamespace(interval.Namespace)
						.From(interval.Min)
						.To(interval.Max)
						.OnlyJumbo()
						.Find(t)).ToListAsync(t);
					reporter.Increment();
					return allChunks;
				}

				var results = await _intervals.ParallelsAsync(loadCollChunks, 16, token);
				_jumboChunks = results.SelectMany(_ => _).ToList();
				reporter.SetCompleteMessage($"found {_jumboChunks.Count} chunks.");
			}
		}

		private async Task  scanJumboChunks(
			IReadOnlyDictionary<CollectionNamespace, ShardedCollectionInfo> collectionsInfo,
			CancellationToken token)
		{
			await using var reporter = _progressRenderer.Start($"Scan jumbo chunks", _jumboChunks.Count);
			{
				async Task scanChunk(Chunk chunk, CancellationToken t)
				{
					var ns = chunk.Namespace;
					var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
					var collInfo = collectionsInfo[ns];

					var result = await db.Datasize(collInfo, chunk, true, t);

					if (!result.IsSuccess)
					{
						Interlocked.Increment(ref _dataSizeCommandErrors);
						_log.Warn("chunk {0} - datasize command fail {1}", chunk.Id, result.ErrorMessage);
					}
					else
					{
						_chunkDataSizes.Add(new ChunkDataSize(ns, chunk.Shard, result.Size, result.NumObjects));
					}

					reporter.Increment();
				}

				await _jumboChunks.ParallelsAsync(scanChunk, 32, token);
			}
		}

		private static readonly List<double> _percentiles = new List<double>()
			{ 0, .50, .75, .90, .95, .99, 1};

		private static readonly List<string> _percentileName = new List<string>()
			{ "min", "50", "75", "90", "95", "99", "max"};

		private void renderPercentiles(IEnumerable<long> sizes)
		{
			var renderedValues = sizes.CalcPercentiles(_percentiles).Zip(_percentileName, (size, name) => $"{name}: {size.ByteSize()}");
			Console.WriteLine("   {0}", string.Join(", ", renderedValues));
		}

		private void renderResults()
		{
			Console.WriteLine();
			Console.WriteLine("Chunk size percentiles.");
			Console.WriteLine();
			Console.WriteLine("By namespaces:");
			foreach (var nsGroup in _chunkDataSizes.GroupBy(_ => _.Ns))
			{
				var group = nsGroup.ToList();
				Console.WriteLine(" * {0} - count: {1}, empty: {2}", nsGroup.Key, group.Count, group.Count(_ => _.Size == 0));
				renderPercentiles(group.Select(_ => _.Size));
			}

			Console.WriteLine();
			Console.WriteLine("By shards:");
			foreach (var shardGroup in _chunkDataSizes.GroupBy(_ => _.Shard))
			{
				var group = shardGroup.ToList();
				Console.WriteLine(" * {0} - count: {1}, empty: {2}", shardGroup.Key, group.Count, group.Count(_ => _.Size == 0));
				renderPercentiles(group.Select(_ => _.Size));
			}

			Console.WriteLine();
			Console.WriteLine("Total - count: {0}, empty: {1}", _chunkDataSizes.Count, _chunkDataSizes.Count(_ => _.Size == 0));
			renderPercentiles(_chunkDataSizes.Select(_ => _.Size));
		}

		public async Task Run(CancellationToken token)
		{
			var collectionsInfo = await _shardedCollectionService.Get(token);

			await findJumboChunks(token);
			await scanJumboChunks(collectionsInfo, token);

			renderResults();
		}

		private class ChunkDataSize
		{
			public CollectionNamespace Ns { get; }
			public ShardIdentity Shard { get; }
			public long Size { get; }
			public long NumObjects { get; }

			public ChunkDataSize(CollectionNamespace ns, ShardIdentity shard, long size, long numObjects)
			{
				Ns = ns;
				Shard = shard;
				Size = size;
				NumObjects = numObjects;
			}
		}

		private static readonly Logger _log = LogManager.GetCurrentClassLogger();
	}
}
