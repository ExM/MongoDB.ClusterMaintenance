using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;
using ShardEqualizer.Config;
using ShardEqualizer.Models;

namespace ShardEqualizer
{
	public class Interval
	{
		public Interval(CollectionNamespace ns, IReadOnlyList<TagIdentity> zones, PreSplitMode preSplit,
			CorrectionMode correction, BsonBound min, BsonBound max, double priority)
		{
			Namespace = ns;
			Zones = zones;
			PreSplit = preSplit;
			Correction = correction;
			Priority = priority;
			Min = min;
			Max = max;
		}

		public Interval(IntervalConfig config)
		{
			Namespace = CollectionNamespace.FromFullName(config.Namespace);
			Zones = config.Zones?.Split(',').Select(_ => new TagIdentity(_)).ToList();
			PreSplit = config.PreSplit ?? PreSplitMode.Auto;
			Correction = config.Correction ?? CorrectionMode.UnShard;
			Priority = config.Priority ?? 1;

			Min = string.IsNullOrWhiteSpace(config.MinBound) ? null : (BsonBound?)BsonBound.Parse(config.MinBound);
			Max = string.IsNullOrWhiteSpace(config.MaxBound) ? null : (BsonBound?)BsonBound.Parse(config.MaxBound);
		}

		public CollectionNamespace Namespace { get; }
		public BsonBound? Min { get; }
		public BsonBound? Max { get; }
		public IReadOnlyList<TagIdentity> Zones { get; }
		public PreSplitMode PreSplit { get; }
		public CorrectionMode Correction { get; }
		public double Priority { get; }
	}
}
