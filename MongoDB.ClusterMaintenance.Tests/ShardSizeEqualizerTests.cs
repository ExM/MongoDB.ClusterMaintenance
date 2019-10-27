using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.ClusterMaintenance.Models;
using MongoDB.ClusterMaintenance.MongoCommands;
using MongoDB.ClusterMaintenance.ShardSizeEqualizing;
using MongoDB.Driver;
using NUnit.Framework;

namespace MongoDB.ClusterMaintenance
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

			var collStatsByShards = new Dictionary<ShardIdentity, CollStats>()
			{
				{shards[0].Id, new CollStats() {Size = 150}},
				{shards[1].Id, new CollStats() {Size = 150}},
				{shards[2].Id, new CollStats() {Size = 350}},
				{shards[3].Id, new CollStats() {Size = 100}},
			};

			var tagRanges = new List<TagRange>()
			{
				new TagRange("tA", testBound(0), testBound(100)),
				new TagRange("tB", testBound(100), testBound(200)),
				new TagRange("tC", testBound(200), testBound(500)),
			};

			var testNS = tagRanges[0].Namespace;
			var chunks = new List<Chunk>();

			for (var i = 0; i < 10; i++)
			{
				chunks.Add(new Chunk()
				{
					Id = $"{testNS.FullName}-{i*10}",
					Namespace = testNS,
					Min = testBound(i * 10),
					Max = testBound((i + 1) * 10),
					Shard = new ShardIdentity("sA")
				});
			}
			for (var i = 10; i < 20; i++)
			{
				chunks.Add(new Chunk()
				{
					Id = $"{testNS.FullName}-{i*10}",
					Namespace = testNS,
					Min = testBound(i * 10),
					Max = testBound((i + 1) * 10),
					Shard = new ShardIdentity("sB")
				});
			}
			for (var i = 20; i < 50; i++)
			{
				chunks.Add(new Chunk()
				{
					Id = $"{testNS.FullName}-{i*10}",
					Namespace = testNS,
					Min = testBound(i * 10),
					Max = testBound((i + 1) * 10),
					Shard = new ShardIdentity("sC")
				});
			}

			var chunkColl = new ChunkCollection(chunks, (ch) => Task.FromResult<long>(10));

			var equalizer = new ShardSizeEqualizer(shards, collStatsByShards, tagRanges, null, chunkColl);

			var round = 0;
			while(await equalizer.Equalize())
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