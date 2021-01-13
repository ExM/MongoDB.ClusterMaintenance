using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace ShardEqualizer
{
	public class VersionRepository
	{
		private readonly IMongoCollection<BsonDocument> _coll;

		public VersionRepository(IMongoDatabase db)
		{
			_coll = db.GetCollection<BsonDocument>("version");
		}

		public async Task<ObjectId> GetClusterId()
		{
			var model =  await _coll.Find(Builders<BsonDocument>.Filter.Eq("_id", 1)).SingleAsync();
			return model["clusterId"].AsObjectId;
		}
	}
}
