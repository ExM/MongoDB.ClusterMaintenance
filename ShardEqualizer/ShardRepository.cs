using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using ShardEqualizer.Models;

namespace ShardEqualizer
{
	public class ShardRepository
	{
		private readonly IMongoCollection<Shard> _coll;

		public ShardRepository(IMongoDatabase db)
		{
			_coll = db.GetCollection<Shard>("shards");
		}

		public async Task<IReadOnlyCollection<Shard>> GetAll(CancellationToken token)
		{
			return await _coll.Find(Builders<Shard>.Filter.Empty).ToListAsync(token);
		}
	}
}
