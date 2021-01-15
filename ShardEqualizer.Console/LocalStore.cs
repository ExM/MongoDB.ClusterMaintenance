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
using ShardEqualizer.JsonSerialization;
using ShardEqualizer.MongoCommands;

namespace ShardEqualizer
{
	public class LocalStore
	{
		private string _fileName;

		private readonly ConcurrentDictionary<CollectionNamespace, CollStatsResult> _map =
			new ConcurrentDictionary<CollectionNamespace, CollStatsResult>();

		public LocalStore()
		{
			var path = Path.GetFullPath(Path.Combine(".", "localStore", "clusterId"));
			if (!Directory.Exists(path))
				Directory.CreateDirectory(path);

			_fileName = Path.Combine(path, "collStats");

			if (File.Exists(_fileName))
			{
				var doc = BsonDocument.Parse(File.ReadAllText(_fileName));
				var container = BsonSerializer.Deserialize<Container>(doc);

				_map = new ConcurrentDictionary<CollectionNamespace, CollStatsResult>(container.Stats);
			}
		}

		private void deserializeContent()
		{

		}

		public CollStatsResult FindCollStats(CollectionNamespace ns)
		{
			return _map.TryGetValue(ns, out var result)
				? result
				: null;
		}

		public void SaveCollStats(CollectionNamespace ns, CollStatsResult collStats)
		{
			_map[ns] = collStats;
		}

		public void SaveFile()
		{
			var container = new Container()
			{
				Date = DateTime.UtcNow,
				Stats = new Dictionary<CollectionNamespace, CollStatsResult>(_map)
			};

			var content = container.ToJson(new JsonWriterSettings() {Indent = true, OutputMode = JsonOutputMode.CanonicalExtendedJson});

			File.WriteAllText(_fileName, content);
		}

		public class Container
		{
			[BsonElement("date")]
			public DateTime Date { get; set; }

			[BsonElement("stats"), BsonDictionaryOptions(DictionaryRepresentation.Document), BsonIgnoreIfNull]
			public Dictionary<CollectionNamespace, CollStatsResult> Stats { get; set; }
		}
	}
}
