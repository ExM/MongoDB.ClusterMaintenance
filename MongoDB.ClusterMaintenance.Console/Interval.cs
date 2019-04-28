using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.ClusterMaintenance.Config;
using MongoDB.ClusterMaintenance.Models;
using MongoDB.Driver;

namespace MongoDB.ClusterMaintenance
{
	public class Interval
	{
		public Interval(CollectionNamespace ns, IReadOnlyList<TagIdentity> zones, PreSplitType preSplit, bool correction, BsonBound min, BsonBound max)
		{
			Namespace = ns;
			Zones = zones;
			PreSplit = preSplit;
			Correction = correction;
			Min = min;
			Max = max;
		}
		
		public Interval(IntervalConfig config, BsonDocument bounds)
		{
			Namespace = CollectionNamespace.FromFullName(config.Namespace);;
			Zones = config.Zones?.Split(',').Select(_ => new TagIdentity(_)).ToList();;
			PreSplit = config.PreSplit;
			Correction = config.Correction;

			if (config.Bounds != null)
			{
				var b = bounds[config.Bounds];
				Min = (BsonBound)b["min"].AsBsonDocument;
				Max = (BsonBound)b["max"].AsBsonDocument;
			}
		}

		public CollectionNamespace Namespace { get; private set; }
		public BsonBound? Min { get; private set; }
		public BsonBound? Max { get; private set; }
		public IReadOnlyList<TagIdentity> Zones { get; private set; }
		public PreSplitType PreSplit { get; private set; }
		public bool Correction { get; private set; }
	}
}