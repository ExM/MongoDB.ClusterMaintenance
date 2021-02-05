using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Bson;
using NUnit.Framework;
using ShardEqualizer.Models;
using ShardEqualizer.ShardSizeEqualizing;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer
{
	[TestFixture]
	public class ShardSizeEqualizerTests
	{
		[Test]
		public async Task Demo()
		{
			var shards = new List<Shard>()
			{
				new Shard("sA", "tA"),
				new Shard("sB", "tB"),
				new Shard("sC", "tC"),
				new Shard("sD", "tD"),
			};

			var collStatsByShards = new Dictionary<ShardIdentity, ShardCollectionStatistics>()
			{
				{shards[0].Id, new ShardCollectionStatistics() {Size = 150}},
				{shards[1].Id, new ShardCollectionStatistics() {Size = 150}},
				{shards[2].Id, new ShardCollectionStatistics() {Size = 350}},
				{shards[3].Id, new ShardCollectionStatistics() {Size = 100}},
			};

			var tagRanges = new List<TagRange>()
			{
				new TagRange("tA", testBound(0), testBound(100)),
				new TagRange("tB", testBound(100), testBound(200)),
				new TagRange("tC", testBound(200), testBound(500)),
			};

			var testNS = tagRanges[0].Namespace;
			var chunks = new List<ChunkInfo>();

			for (var i = 0; i < 10; i++)
			{
				chunks.Add(new ChunkInfo()
				{
					Min = testBound(i * 10),
					Max = testBound((i + 1) * 10),
					Shard = new ShardIdentity("sA")
				});
			}
			for (var i = 10; i < 20; i++)
			{
				chunks.Add(new ChunkInfo()
				{
					Min = testBound(i * 10),
					Max = testBound((i + 1) * 10),
					Shard = new ShardIdentity("sB")
				});
			}
			for (var i = 20; i < 50; i++)
			{
				chunks.Add(new ChunkInfo()
				{
					Min = testBound(i * 10),
					Max = testBound((i + 1) * 10),
					Shard = new ShardIdentity("sC")
				});
			}

			var chunkColl = new ChunkCollection(chunks, (ch) => Task.FromResult<long>(10));

			var targetSize = new Dictionary<TagIdentity, long>()
			{
				{tagRanges[0].Tag, 200},
				{tagRanges[1].Tag, 200},
				{tagRanges[2].Tag, 200},
			};

			var equalizer = new ShardSizeEqualizer(shards, collStatsByShards, tagRanges, targetSize, chunkColl);

			var round = 0;
			while(!(await equalizer.Equalize()).IsSuccess)
			{
				if (equalizer.CurrentSizeDeviation < 3)
					break;

				round++;
			}
		}

		private BsonBound testBound(int x)
		{
			return (BsonBound)new BsonDocument("x", x);
		}
	}
}
