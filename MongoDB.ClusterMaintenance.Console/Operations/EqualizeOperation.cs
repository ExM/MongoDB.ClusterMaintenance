using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using MongoDB.Bson;
using MongoDB.ClusterMaintenance.Config;
using MongoDB.ClusterMaintenance.Models;
using MongoDB.ClusterMaintenance.MongoCommands;
using MongoDB.ClusterMaintenance.ShardSizeEqualizing;
using MongoDB.ClusterMaintenance.WorkFlow;
using MongoDB.Driver;
using NLog;

namespace MongoDB.ClusterMaintenance.Operations
{
	public class EqualizeOperation: IOperation
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();
			
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly IConfigDbRepositoryProvider _configDb;
		private readonly IMongoClient _mongoClient;
		private readonly CommandPlanWriter _commandPlanWriter;
		private readonly long? _moveLimit;
		private readonly bool _planOnly;

		public EqualizeOperation(IConfigDbRepositoryProvider configDb, IReadOnlyList<Interval> intervals,
			IMongoClient mongoClient, CommandPlanWriter commandPlanWriter, long? moveLimit, bool planOnly)
		{
			_configDb = configDb;
			_intervals = intervals;
			_mongoClient = mongoClient;
			_commandPlanWriter = commandPlanWriter;
			_moveLimit = moveLimit;
			_planOnly = planOnly;
		}

		private IReadOnlyCollection<Shard> _shards;
		private long _chunkSize;
		private IList<string> _userDatabases;
		private IList<CollectionNamespace> _allCollectionNames;
		private Dictionary<CollectionNamespace, CollStatsResult> _collStatsMap;
		private IReadOnlyDictionary<CollectionNamespace, List<Chunk>> _chunksByCollection;
		private int _totalChunks = 0;

		private async Task getChunkSize(CancellationToken token)
		{
			_chunkSize = await _configDb.Settings.GetChunksize();
		}
		
		private async Task loadShards(CancellationToken token)
		{
			_shards = await _configDb.Shards.GetAll();
		}

		private async Task loadUserDatabases(CancellationToken token)
		{
			_userDatabases = await _mongoClient.ListUserDatabases(token);
		}
		
		private ObservableTask loadCollections(CancellationToken token)
		{
			async Task<IEnumerable<CollectionNamespace>> listCollectionNames(string dbName, CancellationToken t)
			{
				var db = _mongoClient.GetDatabase(dbName);
				var collNames = await db.ListCollectionNames().ToListAsync(t);
				return collNames.Select(_ => new CollectionNamespace(dbName, _));
			}
			
			return ObservableTask.WithParallels(
				_userDatabases, 
				32, 
				listCollectionNames,
				allCollectionNames => { _allCollectionNames = allCollectionNames.SelectMany(_ => _).ToList(); },
				token);
		}
		
		private ObservableTask loadCollectionStatistics(CancellationToken token)
		{
			async Task<CollStatsResult> runCollStats(CollectionNamespace ns, CancellationToken t)
			{
				var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
				var collStats = await db.CollStats(ns.CollectionName, 1, t);
				return collStats;
			}

			return ObservableTask.WithParallels(
				_allCollectionNames, 
				32, 
				runCollStats,
				allCollStats => { _collStatsMap = allCollStats.ToDictionary(_ => _.Ns); },
				token);
		}
		
		private ObservableTask loadAllCollChunks(CancellationToken token)
		{
			var correctionIntervals = _intervals
				.Where(_ => _.Correction != CorrectionMode.None)
				.ToList();
		
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
				correctionIntervals, 
				32, 
				loadCollChunks,
				chunksByNs => {  _chunksByCollection = chunksByNs.ToDictionary(_ => _.Item1, _ => _.Item2); },
				token);
		}
		
		public async Task Run(CancellationToken token)
		{
			var opList = new WorkList()
			{
				{ "Get chunk size", new SingleWork(getChunkSize, () => _chunkSize.ByteSize())},
				{ "Load shard list", new SingleWork(loadShards, () => $"found {_shards.Count} shards.")},
				{ "Load user databases", new SingleWork(loadUserDatabases, () => $"found {_userDatabases.Count} databases.")},
				{ "Load collections", new ObservableWork(loadCollections, () => $"found {_allCollectionNames.Count} collections.")},
				{ "Load collection statistics", new ObservableWork(loadCollectionStatistics)},
				{ "Load chunks", new ObservableWork(loadAllCollChunks, () => $"found {_totalChunks} chunks.")},
			};

			await opList.Apply(token);

			var unShardedSizeMap = _collStatsMap.Values
				.Where(_ => !_.Sharded)
				.GroupBy(_ => _.Primary)
				.ToDictionary(k => k.Key, g => g.Sum(_ => _.Size));

			var shardByTag =  _intervals
				.SelectMany(_ => _.Zones)
				.Distinct()
				.ToDictionary(_ => _, _ => _shards.Single(s => s.Tags.Contains(_)));
			
			var zoneOpt = new ZoneOptimizationDescriptor(
				_intervals.Where(_ => _.Correction != CorrectionMode.None).Select(_=> _.Namespace),
				_shards.Select(_ => _.Id));

			foreach (var p in unShardedSizeMap)
				zoneOpt.UnShardedSize[p.Key] = p.Value;

			foreach (var coll in zoneOpt.Collections)
			{
				if(!_collStatsMap[coll].Sharded)
					continue;

				foreach (var s in _collStatsMap[coll].Shards)
					zoneOpt[coll, s.Key].CurrentSize = s.Value.Size;
			}

			foreach (var interval in _intervals.Where(_ => _.Correction != CorrectionMode.None))
			{
				var collCfg = zoneOpt.CollectionSettings[interval.Namespace];
				collCfg.UnShardCompensation = interval.Correction == CorrectionMode.UnShard;
				collCfg.Priority = interval.Priority;
				
				var allChunks = _chunksByCollection[interval.Namespace];
				foreach (var tag in interval.Zones)
				{
					var shard = shardByTag[tag].Id;

					var bucket = zoneOpt[interval.Namespace, shard];
					
					bucket.Managed = true;

					var movedChunks = allChunks.Count(_ => _.Shard == shard && !_.Jumbo);
					if (movedChunks <= 1)
					{
						bucket.MinSize = bucket.CurrentSize;
						_log.Info("Disable size reduction {0} on {1}", interval.Namespace, shard);
					}
					else
						bucket.MinSize = bucket.CurrentSize - _chunkSize * (movedChunks - 1);
				}
			}

			var solve = ZoneOptimizationSolve.Find(zoneOpt, token);

			if(!solve.IsSuccess)
				throw new Exception("solution for zone optimization not found");
			
			_log.Info("Found solution with max deviation {0} by shards", solve.TargetShardMaxDeviation.ByteSize());
			_commandPlanWriter.Comment($"Found solution with max deviation {solve.TargetShardMaxDeviation.ByteSize()} by shards");

			foreach(var msg in solve.ActiveConstraints)
				_log.Info("Active constraint: {0}", msg);
			
			foreach (var interval in _intervals.Where(_ => _.Selected).Where(_ => _.Correction != CorrectionMode.None))
			{
				var targetSize = new Dictionary<TagIdentity, long>();

				if (interval.Correction == CorrectionMode.UnShard)
				{
					foreach (var tag in interval.Zones)
					{
						var shId = shardByTag[tag].Id;
						targetSize[tag] = solve[interval.Namespace, shId].TargetSize;
					}
				}
				else
				{
					var collStat = _collStatsMap[interval.Namespace];
					var managedCollSize = interval.Zones.Sum(tag => collStat.Shards[shardByTag[tag].Id].Size);
					var avgZoneSize = managedCollSize / interval.Zones.Count;

					foreach (var tag in interval.Zones)
						targetSize[tag] = avgZoneSize;
				}

				_log.Info("Equalize shards from {0}", interval.Namespace.FullName);
				_commandPlanWriter.Comment($"Equalize shards from {interval.Namespace.FullName}");
				await equalizeShards(interval, _collStatsMap[interval.Namespace], _shards, targetSize, _chunksByCollection[interval.Namespace], token);
			}
		}
		
		private async Task equalizeShards(Interval interval, CollStatsResult collStats,
			IReadOnlyCollection<Shard> shards, IDictionary<TagIdentity, long> targetSize, List<Chunk> allChunks,
			CancellationToken token)
		{
			var tagRanges = await _configDb.Tags.Get(interval.Namespace, interval.Min, interval.Max);

			if (tagRanges.Count == 0)
			{
				_log.Info("tag ranges not found");
				_commandPlanWriter.Comment("no tag ranges");
				_commandPlanWriter.Comment("---");
				return;
			}
			
			var collInfo = await _configDb.Collections.Find(interval.Namespace);
			var db = _mongoClient.GetDatabase(interval.Namespace.DatabaseNamespace.DatabaseName);
			
			async Task<long> chunkSizeResolver(Chunk chunk)
			{
				var result = await db.Datasize(collInfo, chunk, token);
				return result.Size;
			}
			
			var chunkColl = new ChunkCollection(allChunks, chunkSizeResolver);
			var equalizer = new ShardSizeEqualizer(shards, collStats.Shards, tagRanges, targetSize, chunkColl, _moveLimit);

			var lastZone = equalizer.Zones.Last();
			foreach (var zone in equalizer.Zones)
			{
				_log.Info("Zone: {0} Coll: {1} -> {2}",
					zone.Tag, zone.InitialSize.ByteSize(), zone.TargetSize.ByteSize());
				if(zone != lastZone)
					_log.Info("RequireShiftSize: {0} ", zone.Right.RequireShiftSize.ByteSize());
			}
			
			if (_planOnly)
			{
				return;
			}
			
			var rounds = 0;
			var progress = new TargetProgressReporter(equalizer.MovedSize, equalizer.RequireMoveSize, LongExtensions.ByteSize, () =>
			{
				_log.Info("Rounds: {0} SizeDeviation: {1}", rounds, equalizer.CurrentSizeDeviation.ByteSize());
				_log.Info(equalizer.RenderState());
			});
			
			while(await equalizer.Equalize())
			{
				rounds++;
				progress.Update(equalizer.MovedSize);
				token.ThrowIfCancellationRequested();
			}

			await progress.Finalize();
			
			if (rounds == 0)
			{
				_commandPlanWriter.Comment("no correction");
				_commandPlanWriter.Comment("---");
				return;
			}
			
			foreach (var zone in equalizer.Zones)
			{
				_log.Info("Zone: {0} InitialSize: {1} CurrentSize: {2} TargetSize: {3}",
					zone.Tag, zone.InitialSize.ByteSize(), zone.CurrentSize.ByteSize(), zone.TargetSize.ByteSize());
			}
			
			_commandPlanWriter.Comment(equalizer.RenderState());
			_commandPlanWriter.Comment("change tags");

			using (var buffer = new TagRangeCommandBuffer(_commandPlanWriter, interval.Namespace))
			{
				foreach (var tagRange in tagRanges)
					buffer.RemoveTagRange(tagRange.Min, tagRange.Max, tagRange.Tag);

				foreach (var zone in equalizer.Zones)
					buffer.AddTagRange(zone.Min, zone.Max, zone.Tag);
			}

			_commandPlanWriter.Comment("---");
			_commandPlanWriter.Flush();
		}
	}
}