using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using NLog;
using ShardEqualizer.Models;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer.ShardSizeEqualizing
{
	public partial class ShardSizeEqualizer
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();

		private readonly IReadOnlyList<Bound> _movingBounds;

		public ShardSizeEqualizer(IReadOnlyCollection<Shard> shards,
			IReadOnlyDictionary<ShardIdentity, ShardCollectionStatistics> collStatsByShards,
			IReadOnlyList<TagRange> tagRanges,
			IDictionary<TagIdentity, long> targetSize,
			ChunkCollection chunks)
		{
			continuityCheck(tagRanges);

			long sizeByShard(ShardIdentity shard)
			{
				return collStatsByShards.TryGetValue(shard, out var stats) ? stats.Size : 0;
			}

			Zones = tagRanges
				.Select(r => new { tagRange = r, shardId = SelectOnlyOneShardByTag(shards, r.Tag).Id})
				.Select(i => new Zone(i.shardId, i.tagRange, sizeByShard(i.shardId), targetSize[i.tagRange.Tag]))
				.ToList();

			var leftFixedBound = new Bound(this, chunks, tagRanges.First().Min);
			if (leftFixedBound.RightChunk == null)
				throw new Exception($"First chunk not found by first bound of tags");
			Zones.First().Left = leftFixedBound;

			var rightFixedBound = new Bound(this, chunks, tagRanges.Last().Max);
			if(rightFixedBound.LeftChunk == null)
				throw new Exception($"Last chunk not found by last bound of tags");
			Zones.Last().Right = rightFixedBound;

			_movingBounds = tagRanges.Skip(1).Select(item => new Bound(this, chunks, item.Min)).ToList();

			var zoneIndex = 0;
			foreach (var bound in _movingBounds)
			{
				Zones[zoneIndex].Right = bound;
				Zones[zoneIndex + 1].Left = bound;
				zoneIndex++;
			}

			long toRight = 0;
			foreach (var bound in _movingBounds)
			{
				toRight += bound.LeftZone.TargetSize - bound.LeftZone.CurrentSize;
				bound.RequireShiftSize = toRight;
			}
		}

		private static Shard SelectOnlyOneShardByTag(IReadOnlyCollection<Shard> shards, TagIdentity tag)
		{
			using var e = shards.GetEnumerator();
			while (e.MoveNext())
			{
				var result = e.Current;
				if (result.Tags.Contains(tag))
				{
					while (e.MoveNext())
					{
						if (e.Current.Tags.Contains(tag))
							throw new Exception($"shard '{result.Id}' and '{e.Current.Id}' both contains one tag '{tag}'");
					}

					return result;
				}
			}

			throw new Exception($"no shard was found containing the tag '{tag}'");
		}

		public IReadOnlyList<Zone> Zones { get; }

		public long CurrentSizeDeviation
		{
			get
			{
				var minSize = Zones.Select(_ => _.CurrentSize).Min();
				var maxSize = Zones.Select(_ => _.CurrentSize).Max();
				return maxSize - minSize;
			}
		}

		public long MovedSize => _movingBounds.Sum(_ => Math.Abs(_.ShiftSize));

		public long RequireMoveSize => _movingBounds.Sum(_ => Math.Abs(_.RequireShiftSize));

		public long ElapsedShiftSize => _movingBounds.Sum(_ => Math.Abs(_.ElapsedShiftSize));

		public string RenderState()
		{
			var sb = new StringBuilder($"[{_movingBounds.First().LeftZone.Main}]");

			foreach (var bound in _movingBounds)
			{
				var shiftSize = bound.ShiftSize;
				var target = " | ";
				if (shiftSize > 0)
					target = " > ";
				else if (shiftSize < 0)
				{
					target = " < ";
					shiftSize *= -1;
				}

				sb.Append(target);
				sb.Append($"{shiftSize.ByteSize()} ({bound.LeftChunk.Order})");
				sb.Append(target);
				sb.Append($"[{bound.RightZone.Main}]");
			}

			return sb.ToString();
		}

		public async Task<MoveResult> Equalize()
		{
			foreach (var bound in _movingBounds.OrderByDescending(b => b.ElapsedShiftSize))
			{
				var moveResult = await bound.TryMove();
				if (moveResult.IsSuccess)
					return moveResult;
			}

			return MoveResult.Unsuccessful;
		}

		public class MoveResult
		{
			public static readonly MoveResult Unsuccessful = new MoveResult(0);

			public MoveResult(long movedChunkSize)
			{
				IsSuccess = movedChunkSize != 0;
				MovedChunkSize = movedChunkSize;
			}

			public bool IsSuccess { get; }
			public long MovedChunkSize { get; }
		}

		private static void continuityCheck(IReadOnlyCollection<TagRange> tagRanges)
		{
			var nextBound = tagRanges.First().Max;

			foreach (var range in tagRanges.Skip(1))
			{
				if (range.Min != nextBound)
					throw new ArgumentException($"found discontinuity from {range.Min.ToJson()} to {nextBound}");

				nextBound = range.Max;
			}
		}

		public void SetQuotes(Dictionary<ShardIdentity, long?> updateQuotes)
		{
			foreach (var zone in Zones)
			{
				var quoteN = updateQuotes[zone.Main];
				if(quoteN == null)
					continue;

				var quote = quoteN.Value;

				long leftPressure = 0;
				long rightPressure = 0;

				if (zone.Left != null && zone.Left.RequireShiftSize < 0)
					leftPressure = -zone.Left.RequireShiftSize;

				if (zone.Right != null && zone.Right.RequireShiftSize > 0)
					rightPressure = zone.Right.RequireShiftSize;

				if (quote == 0)
				{
					if (leftPressure > 0)
						zone.Left.RequireShiftSize = 0;

					if (rightPressure > 0)
						zone.Right.RequireShiftSize = 0;

					continue;
				}

				if (leftPressure + rightPressure < quote)
				{
					updateQuotes[zone.Main] -= leftPressure + rightPressure;
				}
				else
				{
					if (leftPressure < rightPressure)
					{
						if (rightPressure > quote)
						{
							zone.Right.RequireShiftSize = quote;
						}
						else
						{
							if(leftPressure > 0)
								zone.Left.RequireShiftSize = - (quote - rightPressure);
						}
					}
					else
					{ // rightPressure <= leftPressure

						if (leftPressure > quote)
						{
							zone.Left.RequireShiftSize = -quote;
						}
						else
						{
							if(rightPressure > 0)
								zone.Right.RequireShiftSize = quote - leftPressure;
						}
					}

					updateQuotes[zone.Main] = 0;
				}
			}
		}
	}
}
