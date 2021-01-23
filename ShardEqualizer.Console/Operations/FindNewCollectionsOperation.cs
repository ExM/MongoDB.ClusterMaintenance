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
using ShardEqualizer.ShortModels;

namespace ShardEqualizer.Operations
{
	public class FindNewCollectionsOperation: IOperation
	{
		private readonly ShardListService _shardListService;
		private readonly ShardedCollectionService _shardedCollectionService;
		private readonly CollectionStatisticService _collectionStatisticService;
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly JsonWriterSettings _jsonWriterSettings = new JsonWriterSettings()
			{Indent = false, GuidRepresentation = GuidRepresentation.Unspecified, OutputMode = JsonOutputMode.Shell};

		private IReadOnlyCollection<Shard> _shards;
		private List<string> _allShardNames;
		private string _defaultZones;
		private Dictionary<CollectionNamespace, ShardedCollectionInfo> _shardedCollections;
		private IReadOnlyList<NewShardedCollection> _newShardedCollection;

		public FindNewCollectionsOperation(
			ShardListService shardListService,
			ShardedCollectionService shardedCollectionService,
			CollectionStatisticService collectionStatisticService,
			IReadOnlyList<Interval> intervals)
		{
			_shardListService = shardListService;
			_shardedCollectionService = shardedCollectionService;
			_collectionStatisticService = collectionStatisticService;
			_intervals = intervals;
		}

		private void analyseIntervals()
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

		public async Task Run(CancellationToken token)
		{
			_shards = await _shardListService.Get(token);
			_shardedCollections = new Dictionary<CollectionNamespace, ShardedCollectionInfo>(
				await _shardedCollectionService.Get(token));

			_allShardNames = _shards
				.Select(_ => _.Id.ToString())
				.OrderBy(_ => _)
				.ToList(); //UNDONE find single tag by shard

			_defaultZones = string.Join(",", _allShardNames);

			analyseIntervals();

			var collStats = await _collectionStatisticService.Get(_shardedCollections.Keys, token);

			_newShardedCollection = _shardedCollections.Keys
				.Select(_ => new NewShardedCollection()
				{
					Info = _shardedCollections[_],
					Stats = collStats[_]
				})
				.ToList();

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
			public CollectionStatistics Stats { get; set; }
		}
	}
}
