using System.Runtime.Serialization;

namespace ShardEqualizer.Config
{
	[DataContract(Name = "interval")]
	public class IntervalConfig
	{
		[DataMember(Name = "nameSpace")]
		public string Namespace { get; set; }

		[DataMember(Name = "minBounds")]
		public string MinBound { get; set; }

		[DataMember(Name = "maxBound")]
		public string MaxBound { get; set; }

		[DataMember(Name = "zones")]
		public string Zones { get; set; }

		[DataMember(Name = "preSplit")]
		public PreSplitMode? PreSplit { get; private set; }

		[DataMember(Name = "correction")]
		public CorrectionMode? Correction { get; private set; }

		[DataMember(Name = "Priority")]
		public double? Priority { get; private set;}
	}
}
