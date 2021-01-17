using System;
using System.Collections.Generic;
using System.IO;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using ShardEqualizer.Config;

namespace ShardEqualizer.LocalStoring
{
	public class LocalStoreProvider
	{
		private readonly string _path;
		private readonly bool _readStore;
		private readonly IList<LocalStore> _createdStorages = new List<LocalStore>();

		public LocalStoreProvider(ClusterIdService clusterIdService, LocalStoreConfig localStoreConfig)
		{
			_path = Path.GetFullPath(Path.Combine(".", "localStore", clusterIdService.ClusterId.ToString()));
			if (!Directory.Exists(_path))
				Directory.CreateDirectory(_path);
			_readStore = localStoreConfig.ResetStore != true;
		}

		public LocalStore<T> Create<T>(string storeName, Action<T> onSave) where T : Container, new()
		{
			var fileName = Path.Combine(_path, storeName + ".json");
			T container;
			if (_readStore && File.Exists(fileName))
			{
				var doc = BsonDocument.Parse(File.ReadAllText(fileName));
				container = BsonSerializer.Deserialize<T>(doc);
			}
			else
			{
				container = new T
				{
					Date = DateTime.UtcNow
				};
			}

			var store = new LocalStore<T>(fileName, container, onSave);
			_createdStorages.Add(store);

			return store;
		}

		public void SaveFile()
		{
			foreach (var localStore in _createdStorages)
				localStore.SaveFile();
		}
	}
}