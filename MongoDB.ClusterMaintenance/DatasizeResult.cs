﻿using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDB.ClusterMaintenance
{
	public class DatasizeResult : CommandResult
	{
		[BsonElement("size"), BsonIgnoreIfNull]
		public long Size { get; private set; }
		
		[BsonElement("numObjects"), BsonIgnoreIfNull]
		public long NumObjects { get; private set; }
		
		[BsonElement("millis"), BsonIgnoreIfNull]
		public long Millis { get; private set; }
	}
}