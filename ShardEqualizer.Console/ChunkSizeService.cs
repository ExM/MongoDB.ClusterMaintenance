using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using ShardEqualizer.LocalStoring;
using ShardEqualizer.Models;
using ShardEqualizer.MongoCommands;

namespace ShardEqualizer
{
	public class ChunkSizeService
	{
		private readonly IMongoClient _mongoClient;
		private readonly ShardedCollectionService _shardedCollectionService;
		private readonly LocalStore<ChunkSizeContainer> _store;

		private readonly ConcurrentDictionary<CollectionNamespace, ConcurrentDictionary<ChunkBounds, long>> _map =
			new ConcurrentDictionary<CollectionNamespace, ConcurrentDictionary<ChunkBounds, long>>();

		public ChunkSizeService(
			IMongoClient mongoClient,
			ShardedCollectionService shardedCollectionService,
			LocalStoreProvider storeProvider)
		{
			_mongoClient = mongoClient;
			_shardedCollectionService = shardedCollectionService;

			_store = storeProvider.Create<ChunkSizeContainer>("chunkSizes", onSave);

			if (_store.Container.ChunkSizes != null)
			{
				foreach (var (ns, chunkSizes) in _store.Container.ChunkSizes)
				{
					_map[ns] = new ConcurrentDictionary<ChunkBounds, long>(chunkSizes.Select(
						_ => new KeyValuePair<ChunkBounds, long>(new ChunkBounds(_.Min, _.Max), _.Size)));
				}
			}
		}

		private void onSave(ChunkSizeContainer container)
		{
			container.ChunkSizes = new Dictionary<CollectionNamespace, ChunkSize[]>();

			foreach (var (ns, chunkSizes) in _map)
				container.ChunkSizes[ns] = chunkSizes.Select(_ =>
					new ChunkSize()
					{
						Min = _.Key.Min,
						Max = _.Key.Max,
						Size = _.Value
					}).ToArray();
		}

		public async Task<long> Get(CollectionNamespace ns, BsonBound min, BsonBound max, CancellationToken token)
		{
			var nsMap = _map.GetOrAdd(ns, _ => new ConcurrentDictionary<ChunkBounds, long>());

			var chunksBound = new ChunkBounds(min, max);

			if (nsMap.TryGetValue(chunksBound, out var size))
				return size;

			var collInfo = (await _shardedCollectionService.Get(token))[ns];
			var db = _mongoClient.GetDatabase(ns.DatabaseNamespace.DatabaseName);
			var result = (await db.Datasize(ns, collInfo.Key, min, max, false, token)).Size;
			nsMap[chunksBound] = result;
			_store.OnChanged();
			return result;
		}

		private class ChunkSizeContainer: Container
		{
			[BsonElement("chunkSizes"), BsonDictionaryOptions(DictionaryRepresentation.Document), BsonIgnoreIfNull]
			public Dictionary<CollectionNamespace, ChunkSize[]> ChunkSizes { get; set; }
		}

		private class ChunkSize
		{
			[BsonElement("min"), BsonRequired]
			public BsonBound Min { get; set; }

			[BsonElement("max"), BsonRequired]
			public BsonBound Max { get; set; }

			[BsonElement("size"), BsonRequired]
			public long Size { get; set; }
		}

		private class ChunkBounds: IEquatable<ChunkBounds>
		{
			public ChunkBounds(BsonBound min, BsonBound max)
			{
				Min = min;
				Max = max;
			}

			public BsonBound Min { get; }

			public BsonBound Max { get; }

			public bool Equals(ChunkBounds other)
			{
				if (ReferenceEquals(null, other)) return false;
				if (ReferenceEquals(this, other)) return true;
				return Min.Equals(other.Min) && Max.Equals(other.Max);
			}

			public override bool Equals(object obj)
			{
				if (ReferenceEquals(null, obj)) return false;
				if (ReferenceEquals(this, obj)) return true;
				if (obj.GetType() != this.GetType()) return false;
				return Equals((ChunkBounds) obj);
			}

			public override int GetHashCode()
			{
				return HashCode.Combine(Min, Max);
			}
		}
	}
}
