using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;
using ShardEqualizer.Config;
using ShardEqualizer.Models;

namespace ShardEqualizer
{
	public class Interval
	{
		public Interval(CollectionNamespace ns, IReadOnlyList<TagIdentity> zones, BsonBound min, BsonBound max, bool adjustable)
		{
			Namespace = ns;
			Zones = zones;
			Min = min;
			Max = max;
			Adjustable = adjustable;
		}

		public Interval(IntervalConfig config)
		{
			Namespace = CollectionNamespace.FromFullName(config.Namespace);
			Zones = config.Zones?.Split(',').Select(_ => new TagIdentity(_)).ToList();
			Adjustable = config.Adjustable ?? true;
			Min = string.IsNullOrWhiteSpace(config.MinBound) ? null : (BsonBound?)BsonBound.Parse(config.MinBound);
			Max = string.IsNullOrWhiteSpace(config.MaxBound) ? null : (BsonBound?)BsonBound.Parse(config.MaxBound);
		}

		public CollectionNamespace Namespace { get; }
		public BsonBound? Min { get; }
		public BsonBound? Max { get; }
		public IReadOnlyList<TagIdentity> Zones { get; }
		public bool Adjustable { get; }
	}
}
