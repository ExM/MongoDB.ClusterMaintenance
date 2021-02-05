using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using ShardEqualizer.ChunkCaching;
using ShardEqualizer.LocalStoring;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer
{
	public class ChunkService
	{
		private readonly ChunkRepository _repo;
		private readonly ProgressRenderer _progressRenderer;
		private readonly LocalStore<ChunkContainer> _store;

		private readonly ConcurrentDictionary<CollectionNamespace, ChunksCache> _map = new ConcurrentDictionary<CollectionNamespace, ChunksCache>();

		public ChunkService(
			ChunkRepository repo,
			ProgressRenderer progressRenderer,
			LocalStoreProvider storeProvider)
		{
			_repo = repo;
			_progressRenderer = progressRenderer;

			_store = storeProvider.Create<ChunkContainer>("chunks", onSave);

			if (_store.Container.Chunks != null)
			{
				foreach (var (ns, chunks) in _store.Container.Chunks)
					_map[ns] = new ChunksCache(chunks);
			}
		}

		private void onSave(ChunkContainer container)
		{
			container.Chunks = new Dictionary<CollectionNamespace, ChunkInfo[]>();

			foreach (var (ns, chunks) in _map)
				container.Chunks[ns] = chunks.Array;
		}

		public async Task<IReadOnlyDictionary<CollectionNamespace, ChunksCache>> Get(IEnumerable<CollectionNamespace> nsList, CancellationToken token)
		{
			var nsSet = new HashSet<CollectionNamespace>(nsList);

			var missedKeys = nsSet.Except(_map.Keys).ToList();

			if (missedKeys.Any())
			{
				await using var reporter = _progressRenderer.Start($"Load chunks");
				{
					async Task<(CollectionNamespace ns, long count)> getCounts(CollectionNamespace ns, CancellationToken t)
					{
						var count = await _repo.ByNamespace(ns).Count(t);
						return (ns, count);
					}

					var counts = await missedKeys.ParallelsAsync(getCounts, 32, token);

					reporter.UpdateTotal(counts.Sum(_ => _.Item2));

					var orderedNs = counts.OrderByDescending(_ => _.Item2).ToList();

					async Task getChunks(CollectionNamespace ns, long expectedCount, CancellationToken t)
					{
						var cursor = await _repo.ByNamespace(ns).Find(t);
						var chunks = new List<ChunkInfo>((int)expectedCount);
						using (cursor)
						{
							while (await cursor.MoveNextAsync(t))
							{
								chunks.AddRange(cursor.Current.Select(_ => new ChunkInfo(_)));
								reporter.Increment(cursor.Current.Count());
							}
						}

						_map[ns] = new ChunksCache(chunks.ToArray());
					}

					await orderedNs.ParallelsAsync((i, t) => getChunks(i.ns, i.count, t), 32, token);
				}
				_store.OnChanged();
			}

			return nsSet.ToDictionary(_ => _, _ => _map[_]);
		}

		private class ChunkContainer: Container
		{
			[BsonElement("chunks"), BsonDictionaryOptions(DictionaryRepresentation.Document), BsonIgnoreIfNull]
			public Dictionary<CollectionNamespace, ChunkInfo[]> Chunks { get; set; }
		}
	}
}
