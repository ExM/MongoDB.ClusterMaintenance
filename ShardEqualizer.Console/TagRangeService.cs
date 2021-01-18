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

namespace ShardEqualizer
{
	public class TagRangeService
	{
		private readonly TagRangeRepository _repo;
		private readonly ProgressRenderer _progressRenderer;
		private readonly LocalStore<TagRangeContainer> _store;

		private readonly ConcurrentDictionary<CollectionNamespace, IReadOnlyList<TagRange>> _map = new ConcurrentDictionary<CollectionNamespace, IReadOnlyList<TagRange>>();

		public TagRangeService(
			TagRangeRepository repo,
			ProgressRenderer progressRenderer,
			LocalStoreProvider storeProvider)
		{
			_repo = repo;
			_progressRenderer = progressRenderer;

			_store = storeProvider.Create<TagRangeContainer>("tagRanges", onSave);

			if (_store.Container.TagRanges != null)
				foreach (var (key, value) in _store.Container.TagRanges)
					_map[key] = value;
		}

		private void onSave(TagRangeContainer container)
		{
			container.TagRanges = new Dictionary<CollectionNamespace, IReadOnlyList<TagRange>>(_map);
		}

		public async Task<IReadOnlyDictionary<CollectionNamespace, IReadOnlyList<TagRange>>> Get(IEnumerable<CollectionNamespace> nsList, CancellationToken token)
		{
			var nsSet = new HashSet<CollectionNamespace>(nsList);

			var missedKeys = nsSet.Except(_map.Keys).ToList();

			if (missedKeys.Any())
			{
				await using var reporter = _progressRenderer.Start($"Load tag ranges", missedKeys.Count);
				{
					foreach (var missedKey in missedKeys)
					{
						_map[missedKey] = await _repo.Get(missedKey, token);
						reporter.Increment();
					}
				}
				_store.OnChanged();
			}

			return nsSet.ToDictionary(_ => _, _ => _map[_]);
		}

		private class TagRangeContainer: Container
		{
			[BsonElement("tagRanges"), BsonDictionaryOptions(DictionaryRepresentation.Document), BsonIgnoreIfNull]
			public Dictionary<CollectionNamespace, IReadOnlyList<TagRange>> TagRanges { get; set; }
		}
	}
}
