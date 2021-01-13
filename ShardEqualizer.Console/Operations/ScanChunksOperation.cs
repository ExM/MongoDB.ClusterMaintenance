using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using NLog;
using ShardEqualizer.Models;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.WorkFlow;

namespace ShardEqualizer.Operations
{
	public class ScanChunksOperation : IOperation
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();

		private readonly IConfigDbRepositoryProvider _configDb;
		private readonly IMongoClient _mongoClient;
		private readonly List<long> _sizeBounds;
		private readonly IEnumerable<string> _headers;
		private readonly IReadOnlyList<Interval> _intervals;
		private Dictionary<CollectionNamespace, ShardedCollectionInfo> _collectionsInfo;
		private int _totalChunks;
		private Dictionary<CollectionNamespace, List<Chunk>> _chunksByCollection;

		public ScanChunksOperation(IReadOnlyList<Interval> intervals, IConfigDbRepositoryProvider configDb, IMongoClient mongoClient, IList<string> sizeLabels)
		{
			_configDb = configDb;
			_mongoClient = mongoClient;

			_sizeBounds = sizeLabels.Select(BinaryPrefix.Parse).ToList();
			_headers = new [] {"shard", "jumbo", "empty"}.Concat(sizeLabels).Concat(new [] {"Max"});

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

		private ObservableTask loadAllCollChunks(CancellationToken token)
		{
			async Task<Tuple<CollectionNamespace, List<Chunk>>> loadCollChunks(Interval interval, CancellationToken t)
			{
				var allChunks = await (await _configDb.Chunks
					.ByNamespace(interval.Namespace)
					.From(interval.Min)
					.To(interval.Max)
					.Find()).ToListAsync(t);
				Interlocked.Add(ref _totalChunks, allChunks.Count);
				return new Tuple<CollectionNamespace, List<Chunk>>(interval.Namespace, allChunks);
			}

			return ObservableTask.WithParallels(
				_intervals,
				8,
				loadCollChunks,
				chunksByNs => {  _chunksByCollection = chunksByNs.ToDictionary(_ => _.Item1, _ => _.Item2); },
				token);
		}

		public async Task Run(CancellationToken token)
		{
			var scanList = new WorkList();

			foreach (var interval in _intervals)
			{
				scanList.Add(
					$"Scan collection {interval.Namespace}",
					new SingleWork(t => scanInterval(interval.Namespace, t)));
			}

			var opList = new WorkList()
			{
				{ "Load collections info", new ObservableWork(loadCollectionsInfo)},
				{ "Load chunks", new ObservableWork(loadAllCollChunks, () => $"found {_totalChunks} chunks.")},
				{ "Scan collections", scanList}
			};

			await opList.Apply(token);
		}

		private async Task<string> scanInterval(CollectionNamespace ns, CancellationToken token)
		{
			var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
			var collInfo = _collectionsInfo[ns];
			var chunks = _chunksByCollection[ns];

			var chunkCountByShards = new ConcurrentDictionary<ShardIdentity, ChunkCounts>();

			var progressReporter = new TargetProgressReporter(0, chunks.Count);
			var index = 0;
			foreach (var chunk in chunks)
			{
				_log.Debug("Process chunk: {0}/{1}", chunk.Id, chunk.Shard);

				var result = await db.Datasize(collInfo, chunk, true, token);

				if (result.IsSuccess)
				{
					_log.Debug("chunk: {0}/{1} size: {2}", chunk.Id, chunk.Shard, result.Size);

					chunkCountByShards
						.GetOrAdd(chunk.Shard, _ => new ChunkCounts(_sizeBounds))
						.Increment(chunk.Jumbo, result.Size);
				}
				else
					_log.Warn("datasize command fail");

				progressReporter.Update(++index);
				progressReporter.TryRender(() => renderReport(chunkCountByShards));

				if(token.IsCancellationRequested)
					break;
			}

			await progressReporter.Stop();

			token.ThrowIfCancellationRequested();

			var sb = new StringBuilder();
			sb.AppendLine("done");
			foreach (var line in renderReport(chunkCountByShards))
				sb.AppendLine(line);

			return sb.ToString();
		}

		private string[] renderReport(ConcurrentDictionary<ShardIdentity, ChunkCounts> chunkCountByShards)
		{
			var result = new List<string>();
			result.Add(string.Join("; ", _headers));

			foreach (var pair in chunkCountByShards.ToArray())
				result.Add($"{pair.Key}; {pair.Value.Render()}");

			return result.ToArray();
		}

		private class ChunkCounts
		{
			private long _jumbo;
			private long _empty;
			private long _max;
			private readonly IList<long> _sizesBounds;
			private readonly long[] _byBounds;

			public ChunkCounts(IList<long> sizesBounds)
			{
				_sizesBounds = sizesBounds;
				_byBounds = new long[sizesBounds.Count];
			}

			public string Render()
			{
				lock (this)
				{
					var cells = new [] {_jumbo, _empty}.Concat(_byBounds).Concat(new [] {_max});
					return string.Join("; ", cells.Select(_ => _.ToString()));
				}
			}

			public void Increment(bool jumbo, long size)
			{
				var index = -1;
				if (0 < size)
				{
					var pos = 0;
					foreach (var bound in _sizesBounds)
					{
						if (size < bound)
						{
							index = pos;
							break;
						}

						pos++;
					}
				}

				lock (this)
				{
					if (jumbo)
						_jumbo++;

					if (size == 0)
						_empty++;
					else if (index >= 0)
						_byBounds[index]++;
					else
						_max++;
				}
			}
		}
	}
}
