using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using ShardEqualizer.Models;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer.MongoCommands
{
	public static class MongoCommandExtensions
	{
		public static Task<DatasizeResult> Datasize(this IMongoDatabase db, ShardedCollectionInfo collInfo, Chunk chunk, bool estimate, CancellationToken token)
		{
			return db.Datasize(collInfo.Id, collInfo.Key, chunk.Min, chunk.Max, estimate, token);
		}

		public static Task<DatasizeResult> Datasize(this IMongoDatabase db, ShardedCollectionInfo collInfo, ChunkInfo chunk, bool estimate, CancellationToken token)
		{
			return db.Datasize(collInfo.Id, collInfo.Key, chunk.Min, chunk.Max, estimate, token);
		}

		public static async Task<DatasizeResult> Datasize(this IMongoDatabase db, CollectionNamespace ns, BsonDocument key, BsonBound min, BsonBound max, bool estimate, CancellationToken token)
		{
			var cmd = new BsonDocument
			{
				{ "datasize", ns.FullName },
				{ "keyPattern", key },
				{ "min", (BsonDocument)min },
				{ "max", (BsonDocument)max },
				{ "estimate", estimate}
			};

			var result = await db.RunCommandAsync<DatasizeResult>(cmd, ReadPreference.SecondaryPreferred, token);
			result.EnsureSuccess();
			return result;
		}

		public static async Task<DbStatsResult> DbStats(this IMongoDatabase db, int scale, CancellationToken token)
		{
			var cmd =  new BsonDocument
			{
				{ "dbStats", 1 },
				{ "scale", scale},
			};

			var result = await db.RunCommandAsync<DbStatsResult>(cmd, ReadPreference.SecondaryPreferred, token);
			result.EnsureSuccess();
			return result;
		}

		public static async Task<CollStatsResult> CollStats(this IMongoDatabase db, string collectionName, int scale, CancellationToken token)
		{
			try
			{
				var cmd =  new BsonDocument
				{
					{ "collStats", collectionName },
					{ "scale", scale},
				};

				var result = await db.RunCommandAsync<CollStatsResult>(cmd, ReadPreference.SecondaryPreferred, token);
				result.EnsureSuccess();
				return result;
			}
			catch (Exception ex)
			{
				throw new Exception($"Error while collStats command within database: {db.DatabaseNamespace.DatabaseName} collection: {collectionName}", ex);
			}
		}
	}
}
