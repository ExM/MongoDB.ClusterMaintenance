using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace ShardEqualizer
{
	public class SettingsRepository
	{
		private readonly IMongoCollection<BsonDocument> _coll;

		public SettingsRepository(IMongoDatabase db)
		{
			_coll = db.GetCollection<BsonDocument>("settings");
		}

		public async Task<long> GetChunksize(CancellationToken token)
		{
			var model =  await _coll.Find(Builders<BsonDocument>.Filter.Eq("_id", "chunksize")).SingleAsync(token);
			var sizeInMb = model["value"].AsInt64;
			return sizeInMb * ScaleSuffix.Mega.Factor();
		}
	}
}
