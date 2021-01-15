using System.Collections.Generic;
using MongoDB.Driver;

namespace ShardEqualizer
{
	public class UserDatabases : List<DatabaseNamespace>
	{
		public UserDatabases(IEnumerable<DatabaseNamespace> items) : base(items)
		{
		}
	}
}