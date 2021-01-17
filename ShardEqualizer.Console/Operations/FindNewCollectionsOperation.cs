using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using NLog;
using ShardEqualizer.Models;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.WorkFlow;

namespace ShardEqualizer.Operations
{
	public class FindNewCollectionsOperation: IOperation
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();

		private readonly IConfigDbRepositoryProvider _configDb;
		private readonly IMongoClient _mongoClient;
		private readonly IDataSource<AllShards> _allShardsSource;
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly JsonWriterSettings _jsonWriterSettings = new JsonWriterSettings()
			{Indent = false, GuidRepresentation = GuidRepresentation.Unspecified, OutputMode = JsonOutputMode.Shell};

		private IReadOnlyCollection<Shard> _shards;
		private List<string> _allShardNames;
		private string _defaultZones;
		private Dictionary<CollectionNamespace, ShardedCollectionInfo> _shardedCollections;
		private IReadOnlyList<NewShardedCollection> _newShardedCollection;

		public FindNewCollectionsOperation(
			IDataSource<AllShards> allShardsSource,
			IConfigDbRepositoryProvider configDb,
			IMongoClient mongoClient,
			IReadOnlyList<Interval> intervals)
		{
			_allShardsSource = allShardsSource;
			_intervals = intervals;
			_configDb = configDb;
			_mongoClient = mongoClient;
		}

		private async Task<string> loadShardedCollections(CancellationToken token)
		{
			_shardedCollections = (await _configDb.Collections.FindAll()).ToDictionary(_ => _.Id);

			return $"found {_shardedCollections.Count} collections.";
		}

		private void analizeIntervals(CancellationToken token)
		{
			foreach (var ns in _intervals.Select(_ => _.Namespace))
			{
				if (_shardedCollections.TryGetValue(ns, out var shardedCollection))
				{
					if(shardedCollection.Dropped)
						Console.WriteLine("\tcollection '{0}' dropped", ns);

					_shardedCollections.Remove(ns);
				}
				else
				{
					Console.WriteLine("\tcollection '{0}' not sharded", ns);
				}
			}

			foreach (var ns in _shardedCollections.Keys.ToList())
			{
				if(_shardedCollections[ns].Dropped)
					_shardedCollections.Remove(ns);
			}
		}

		private ObservableTask loadCollectionStatistics(CancellationToken token)
		{
			async Task<NewShardedCollection> runCollStats(ShardedCollectionInfo shardedCollection, CancellationToken t)
			{
				var ns = shardedCollection.Id;
				var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
				var collStats = await db.CollStats(ns.CollectionName, 1, t);

				return new NewShardedCollection()
				{
					Info = shardedCollection,
					Stats = collStats
				};
			}

			return ObservableTask.WithParallels(
				_shardedCollections.Values.ToList(),
				32,
				runCollStats,
				newShardedCollection => { _newShardedCollection = newShardedCollection; },
				token);
		}

		public async Task Run(CancellationToken token)
		{
			_shards = await _allShardsSource.Get(token);

			_allShardNames = _shards
				.Select(_ => _.Id.ToString())
				.OrderBy(_ => _)
				.ToList();

			_defaultZones = string.Join(",", _allShardNames);

			var opList = new WorkList()
			{
				{ "Load sharded collections", new SingleWork(loadShardedCollections)},
				{ "Analyse of loaded data", analizeIntervals},
				{ "Load collection statistics", new ObservableWork(loadCollectionStatistics)},
			};

			await opList.Apply(token);

			if (_newShardedCollection.Count == 0)
			{
				Console.WriteLine("new sharded collections not found");
				return;
			}

			var sb = new StringBuilder();

			foreach (var newShardedCollection in _newShardedCollection)
			{
				var totalSize = newShardedCollection.Stats.Size;
				if (totalSize == 0)
					totalSize = 1;

				var distributionMap = _shards.ToDictionary(_ => _.Id, _ => 0.0);
				foreach (var pair in newShardedCollection.Stats.Shards)
					distributionMap[pair.Key] = (double) pair.Value.Size * 100 / totalSize;

				var distribution = distributionMap
					.OrderBy(_ => _.Key)
					.Select(_ => $"{_.Value:F0}%");

				sb.AppendLine();
				sb.AppendLine($"\t<!-- totalSize: {newShardedCollection.Stats.Size.ByteSize()} storageSize: {newShardedCollection.Stats.StorageSize.ByteSize()} distribution: {string.Join(" ", distribution)} -->");
				sb.AppendLine($"\t<!-- key: {newShardedCollection.Info.Key.ToJson(_jsonWriterSettings)} -->");
				sb.AppendLine($"\t<Interval nameSpace=\"{newShardedCollection.Info.Id}\" bounds=\"\"");
				sb.AppendLine($"\t\tpreSplit=\"chunks\"\tcorrection=\"unShard\"\tzones=\"{_defaultZones}\" />");
			}

			Console.WriteLine("New intervals:");
			Console.WriteLine(sb);
		}

		public class NewShardedCollection
		{
			public ShardedCollectionInfo Info { get; set; }
			public CollStatsResult Stats { get; set; }
		}
	}
}
