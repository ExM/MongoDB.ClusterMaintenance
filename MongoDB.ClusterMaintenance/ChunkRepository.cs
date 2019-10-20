using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.ClusterMaintenance.Models;
using MongoDB.Driver;

namespace MongoDB.ClusterMaintenance
{
	public class ChunkRepository
	{
		private readonly IMongoCollection<Chunk> _coll;

		internal ChunkRepository(IMongoDatabase db)
		{
			_coll = db.GetCollection<Chunk>("chunks");
		}
		
		public Task<Chunk> Find(string id)
		{
			return _coll.Find(_ => _.Id == id).SingleOrDefaultAsync();
		}

		public Filtered ByNamespace(CollectionNamespace ns)
		{
			return new Filtered(_coll, Builders<Chunk>.Filter.Eq(_ => _.Namespace, ns));
		}
		
		public class Filtered
		{
			private readonly IMongoCollection<Chunk> _coll;
			private readonly FilterDefinition<Chunk> _filter;

			internal Filtered(IMongoCollection<Chunk> coll, FilterDefinition<Chunk> filter)
			{
				_coll = coll;
				_filter = filter;
			}

			public async Task<IAsyncCursor<Chunk>> Find()
			{
				return await _coll.FindAsync(_filter, new FindOptions<Chunk>()
				{
					Sort = Builders<Chunk>.Sort
						.Ascending(_ => _.Namespace)
						.Ascending(_ => _.Min)
				});
			}

			public async Task<long> Count()
			{
				return await _coll.CountDocumentsAsync(_filter);
			}
			
			public Filtered From(BsonBound? from)
			{
				if (from == null)
					return this;

				return new Filtered(_coll, _filter & Builders<Chunk>.Filter.Gte(_ => _.Min, from));
			}
			
			public Filtered To(BsonBound? to)
			{
				if (to == null)
					return this;

				return new Filtered(_coll, _filter & Builders<Chunk>.Filter.Lt(_ => _.Min, to));
			}
			
			public Filtered NoJumbo()
			{
				return new Filtered(_coll, _filter & Builders<Chunk>.Filter.Where(_ => _.Jumbo != true));
			}
			
			public Filtered ExcludeShards(IEnumerable<ShardIdentity> shards)
			{
				return new Filtered(_coll, _filter & Builders<Chunk>.Filter.Nin(_ => _.Shard, shards));
			}
		}
	}
}