using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using ShardEqualizer.Models;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer
{
	public class ChunkCollection
	{
		private readonly IDictionary<BsonBound, Entry> _maxMap = new SortedDictionary<BsonBound, Entry>();
		private readonly IDictionary<BsonBound, Entry> _minMap = new SortedDictionary<BsonBound, Entry>();

		private readonly IReadOnlyList<Entry> _chunks;

		public ChunkCollection(IReadOnlyList<ChunkInfo> chunks, Func<ChunkInfo, Task<long>> chunkSizeResolver)
		{
			_chunks = chunks.Select((c, o) => new Entry(o, c, chunkSizeResolver)).ToList();

			var firstChunk = _chunks.First();
			_minMap.Add(firstChunk.Chunk.Min, firstChunk);
			_maxMap.Add(firstChunk.Chunk.Max, firstChunk);
			var nextBound = firstChunk.Chunk.Max;

			foreach (var chunk in _chunks.Skip(1))
			{
				if (chunk.Chunk.Min != nextBound)
					throw new ArgumentException($"found discontinuity from {chunk.Chunk.Min.ToJson()} to {nextBound}");
				nextBound = chunk.Chunk.Max;
				_minMap.Add(chunk.Chunk.Min, chunk);
				_maxMap.Add(chunk.Chunk.Max, chunk);
			}
		}

		public Entry FindRight(Entry value)
		{
			if (value.Order >= _chunks.Count - 1)
				return null;

			return _chunks[value.Order + 1];
		}

		public Entry FindLeft(Entry value)
		{
			if (value.Order <= 0)
				return null;

			return _chunks[value.Order - 1];
		}

		public Entry FindRight(BsonBound value)
		{
			return _minMap.TryGetValue(value, out var result) ? result : null;
		}

		public Entry FindLeft(BsonBound value)
		{
			return _maxMap.TryGetValue(value, out var result) ? result : null;
		}

		public class Entry
		{
			private readonly Func<ChunkInfo, Task<long>> _chunkSizeResolver;
			private volatile Task<long> _sizeTask;

			public Entry(int order, ChunkInfo chunk, Func<ChunkInfo, Task<long>> chunkSizeResolver)
			{
				_chunkSizeResolver = chunkSizeResolver;
				Order = order;
				Chunk = chunk;
			}

			public int Order { get; }
			public ChunkInfo Chunk { get; }

			public Task<long> Size
			{
				get
				{
					if (_sizeTask == null)
						_sizeTask = _chunkSizeResolver(Chunk);

					return _sizeTask;
				}
			}
		}
	}
}
