using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using ShardEqualizer.Models;
using ShardEqualizer.MongoCommands;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer
{
	public class LocalStore
	{
		private readonly string _fileName;

		private readonly ConcurrentDictionary<CollectionNamespace, CollectionStatistics> _map =
			new ConcurrentDictionary<CollectionNamespace, CollectionStatistics>();

		public LocalStore(ClusterIdService clusterIdService)
		{
			var path = Path.GetFullPath(Path.Combine(".", "localStore", clusterIdService.ClusterId.ToString()));
			if (!Directory.Exists(path))
				Directory.CreateDirectory(path);

			_fileName = Path.Combine(path, "collStats");

			if (File.Exists(_fileName))
			{
				var doc = BsonDocument.Parse(File.ReadAllText(_fileName));
				var container = BsonSerializer.Deserialize<Container>(doc);

				_map = new ConcurrentDictionary<CollectionNamespace, CollectionStatistics>(container.Stats);
			}
		}

		public CollectionStatistics FindCollStats(CollectionNamespace ns)
		{
			return _map.TryGetValue(ns, out var result)
				? result
				: null;
		}

		public void SaveCollStats(CollectionNamespace ns, CollectionStatistics collStats)
		{
			_map[ns] = collStats;
		}

		public void SaveFile()
		{
			var container = new Container()
			{
				Date = DateTime.UtcNow,
				Stats = new Dictionary<CollectionNamespace, CollectionStatistics>(_map)
			};

			var content = container.ToJson(new JsonWriterSettings() {Indent = true, OutputMode = JsonOutputMode.CanonicalExtendedJson});

			File.WriteAllText(_fileName, content);
		}

		public class Container
		{
			[BsonElement("date")]
			public DateTime Date { get; set; }

			[BsonElement("stats"), BsonDictionaryOptions(DictionaryRepresentation.Document), BsonIgnoreIfNull]
			public Dictionary<CollectionNamespace, CollectionStatistics> Stats { get; set; }
		}
	}
}
