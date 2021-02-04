using System.Runtime.Serialization;

namespace ShardEqualizer.Config
{
	[DataContract(Name = "interval")]
	public class IntervalConfig
	{
		[DataMember(Name = "nameSpace")]
		public string Namespace { get; set; }

		[DataMember(Name = "minBound")]
		public string MinBound { get; set; }

		[DataMember(Name = "maxBound")]
		public string MaxBound { get; set; }

		[DataMember(Name = "zones")]
		public string Zones { get; set; }

		[DataMember(Name = "adjustable")]
		public bool? Adjustable { get; set; }
	}
}
