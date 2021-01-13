using System.Runtime.Serialization;

namespace ShardEqualizer.Config
{
	[DataContract(Name = "Clusters")]
	public class ClustersConfig
	{
		[DataMember(Name = "Default")]
		public string Default { get; private set; }
	}
}
