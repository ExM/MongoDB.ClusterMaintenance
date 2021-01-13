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
	public class ScanJumboChunksOperation : IOperation
	{
		private readonly IConfigDbRepositoryProvider _configDb;
		private readonly IMongoClient _mongoClient;
		private readonly IReadOnlyList<Interval> _intervals;
		private Dictionary<CollectionNamespace, ShardedCollectionInfo> _collectionsInfo;
		private List<Chunk> _jumboChunks;
		private ConcurrentBag<ChunkDataSize> _chunkDataSizes = new ConcurrentBag<ChunkDataSize>();
		private int _dataSizeCommandErrors = 0;

		public ScanJumboChunksOperation(IReadOnlyList<Interval> intervals, IConfigDbRepositoryProvider configDb, IMongoClient mongoClient)
		{
			_configDb = configDb;
			_mongoClient = mongoClient;

			if (intervals.Count == 0)
				throw new ArgumentException("interval list is empty");

			_intervals = intervals;
		}

		private ObservableTask loadCollectionsInfo(CancellationToken token)
		{
			async Task<ShardedCollectionInfo> loadCollectionInfo(Interval interval, CancellationToken t)
			{
				var collInfo = await _configDb.Collections.Find(interval.Namespace);
				if (collInfo == null)
					throw new InvalidOperationException($"collection {interval.Namespace.FullName} not sharded");
				return collInfo;
			}

			return ObservableTask.WithParallels(
				_intervals,
				8,
				loadCollectionInfo,
				allCollectionsInfo => { _collectionsInfo = allCollectionsInfo.ToDictionary(_ => _.Id); },
				token);
		}

		private ObservableTask findJumboChunks(CancellationToken token)
		{
			async Task<List<Chunk>> loadCollChunks(Interval interval, CancellationToken t)
			{
				var allChunks = await (await _configDb.Chunks
					.ByNamespace(interval.Namespace)
					.From(interval.Min)
					.To(interval.Max)
					.OnlyJumbo()
					.Find()).ToListAsync(t);
				return allChunks;
			}

			return ObservableTask.WithParallels(
				_intervals,
				8,
				loadCollChunks,
				chunks => {  _jumboChunks = chunks.SelectMany(_ => _).ToList(); },
				token);
		}

		private ObservableTask scanJumboChunks(CancellationToken token)
		{
			async Task scanChunk(Chunk chunk, CancellationToken t)
			{
				var ns = chunk.Namespace;
				var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
				var collInfo = _collectionsInfo[ns];

				var result = await db.Datasize(collInfo, chunk, true, token);

				if (!result.IsSuccess)
				{
					Interlocked.Increment(ref _dataSizeCommandErrors);
					_log.Warn("chunk {0} - datasize command fail {1}", chunk.Id, result.ErrorMessage);
				}
				else
				{
					_chunkDataSizes.Add(new ChunkDataSize(ns, chunk.Shard, result.Size, result.NumObjects));
				}
			}

			return ObservableTask.WithParallels(
				_jumboChunks,
				32,
				scanChunk,
				token);
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
			var opList = new WorkList()
			{
				{ "Load collections info", new ObservableWork(loadCollectionsInfo)},
				{ "Find jumbo chunks", new ObservableWork(findJumboChunks, () => $"found {_jumboChunks.Count} chunks.")},
				{ "Scan jumbo chunks", new ObservableWork(scanJumboChunks)}
			};

			await opList.Apply(token);

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
