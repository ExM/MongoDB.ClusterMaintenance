using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.ClusterMaintenance.Models;

namespace MongoDB.ClusterMaintenance.ShardSizeEqualizing
{
	public partial class ShardSizeEqualizer
	{
		public class Bound
		{
			private readonly ChunkCollection _chunks;
			public Zone LeftZone { get; set; }
			public ChunkCollection.Entry LeftChunk { get; private set; }
			public Zone RightZone { get; set; }
			public ChunkCollection.Entry RightChunk { get; private set; }
			public BsonBound Value { get; private set; }
			public long ShiftSize => _shiftSize;
			private long _shiftSize = 0;
			
			public long RequireShiftSize = 0;
			
			public long ElapsedShiftSize => Math.Abs(_shiftSize - RequireShiftSize);
			
			private ChunkCollection.Entry _nextChunk;

			internal Bound(ChunkCollection chunks, BsonBound value)
			{
				Value = value;
				_chunks = chunks;
				LeftChunk = _chunks.FindLeft(value);
				RightChunk =_chunks.FindRight(value);
			}

			public async Task<bool> TryMove()
			{
				if (RequireShiftSize == 0)
					return false;

				if (RequireShiftSize < 0)
				{ // to left
					if (_shiftSize <= RequireShiftSize)
						return false;
				
					if (_nextChunk == null)
						_nextChunk = await findLeftNextChunk();

					if (_nextChunk == null)
						return false;
					
					var nextChunkSize = await _nextChunk.Size;
				
					if ((_shiftSize - nextChunkSize/2) < RequireShiftSize)
						return false;

					Interlocked.Add(ref _shiftSize, -nextChunkSize);
					LeftZone.SizeDown(nextChunkSize);
					RightZone.SizeUp(nextChunkSize);
					RightChunk = _nextChunk;
					Value = _nextChunk.Chunk.Min;
					LeftChunk = _chunks.FindLeft(_nextChunk);
				}
				else
				{ // to right
					if (RequireShiftSize <= _shiftSize)
						return false;
				
					if (_nextChunk == null)
						_nextChunk = await findRightNextChunk();

					if (_nextChunk == null)
						return false;
					
					var nextChunkSize = await _nextChunk.Size;
					
					if ((_shiftSize + nextChunkSize/2) > RequireShiftSize)
						return false;
					
					Interlocked.Add(ref _shiftSize, nextChunkSize);
					LeftZone.SizeUp(nextChunkSize);
					RightZone.SizeDown(nextChunkSize);
					LeftChunk = _nextChunk;
					Value = _nextChunk.Chunk.Max;
					RightChunk = _chunks.FindRight(_nextChunk);
				}
				
				_nextChunk = null;
				return true;
			}
			
			private async Task<ChunkCollection.Entry> findLeftNextChunk()
			{
				var stopEntry = LeftZone.Left.RightChunk;
				var candidate = LeftChunk;

				while (true)
				{
					if (candidate == stopEntry)
						return null;

					if (!candidate.Chunk.Jumbo && await candidate.Size > 0)
						return candidate;

					candidate = _chunks.FindLeft(candidate);
					if (candidate == null)
						return null;
				}
			}

			private async Task<ChunkCollection.Entry> findRightNextChunk()
			{
				var stopEntry =  RightZone.Right.LeftChunk;
				var candidate = RightChunk;

				while (true)
				{
					if (candidate == stopEntry)
						return null;

					if (!candidate.Chunk.Jumbo && await candidate.Size > 0)
						return candidate;

					candidate = _chunks.FindRight(candidate);
					if (candidate == null)
						return null;
				}
			}
		}
	}
}