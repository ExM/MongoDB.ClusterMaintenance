using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using ShardEqualizer.JsonSerialization;
using ShardEqualizer.Models;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer.Operations
{
	public class ConfigUpdateOperation: IOperation
	{
		private readonly ShardedCollectionService _shardedCollectionService;
		private readonly CollectionStatisticService _collectionStatisticService;
		private readonly IReadOnlyList<Interval> _intervals;
		private readonly JsonWriterSettings _jsonWriterSettings = new JsonWriterSettings()
			{Indent = false, GuidRepresentation = GuidRepresentation.Unspecified, OutputMode = JsonOutputMode.Shell};

		private Dictionary<CollectionNamespace, ShardedCollectionInfo> _shardedCollections;
		private IReadOnlyList<NewShardedCollection> _newShardedCollection;

		public ConfigUpdateOperation(
			ShardedCollectionService shardedCollectionService,
			CollectionStatisticService collectionStatisticService,
			IReadOnlyList<Interval> intervals)
		{
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
			_shardedCollections = new Dictionary<CollectionNamespace, ShardedCollectionInfo>(
				await _shardedCollectionService.Get(token));

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
				sb.AppendLine();
				sb.AppendLine($"\t<!-- totalSize: {newShardedCollection.Stats.Size.ByteSize()} " +
				              $"storageSize: {newShardedCollection.Stats.StorageSize.ByteSize()} " +
				              $"key: {ShellJsonWriter.AsJson(newShardedCollection.Info.Key)} -->");
				sb.AppendLine($"\t<Interval nameSpace=\"{newShardedCollection.Info.Id}\" />");
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
