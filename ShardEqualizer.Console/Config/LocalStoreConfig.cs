using System.Runtime.Serialization;

namespace ShardEqualizer.Config
{
	[DataContract(Name = "LocalStore")]
	public class LocalStoreConfig
	{
		[DataMember(Name = "Reset")]
		public bool? ResetStore { get; set; }
	}
}
