using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using ShardEqualizer.Models;

namespace ShardEqualizer
{
	public class TagRangeRepository
	{
		private readonly IMongoCollection<TagRange> _coll;

		public TagRangeRepository(IMongoDatabase db)
		{
			_coll = db.GetCollection<TagRange>("tags");
		}

		public async Task<IReadOnlyList<TagRange>> Get(CollectionNamespace ns)
		{
			return await _coll
				.Find(_ => _.Namespace == ns)
				.Sort(Builders<TagRange>.Sort.Ascending(_ => _.Id))
				.ToListAsync();
		}
		public async Task<IReadOnlyList<TagRange>> Get(CollectionNamespace ns, CancellationToken token)
		{
			return await _coll
				.Find(_ => _.Namespace == ns)
				.Sort(Builders<TagRange>.Sort.Ascending(_ => _.Id))
				.ToListAsync(token);
		}


		public async Task<IReadOnlyList<TagRange>> Get(CollectionNamespace ns, BsonBound? intL, BsonBound? intR)
		{
			var tagRanges = await _coll
				.Find(_ => _.Namespace == ns)
				.Sort(Builders<TagRange>.Sort.Ascending(_ => _.Id))
				.ToListAsync();

			if(intL.HasValue && intR.HasValue)
				tagRanges = tagRanges.Where(r => crossInterval(intL.Value, intR.Value, r.Min, r.Max)).ToList();

			return tagRanges;
		}

		private static bool crossInterval(BsonBound intL, BsonBound intR, BsonBound chL, BsonBound chR)
		{
			return chL <= intR  && intL < chR;
		}
	}
}
