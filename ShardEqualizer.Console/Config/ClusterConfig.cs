using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using NConfiguration.Combination;
using NConfiguration.Combination.Collections;

namespace ShardEqualizer.Config
{
	public class ClusterConfig
	{
		[DataMember(Name = "Servers")]
		private string _servers
		{
			get => Servers == null ? null : string.Join(",", Servers);
			set => Servers = value?.Split(',').ToList();
		}

		[IgnoreDataMember]
		public IReadOnlyList<string> Servers { get; private set; }

		[DataMember(Name = "User")]
		public string User { get; private set; }

		[DataMember(Name = "Password")]
		public string Password { get; private set; }

		public bool IsRequireAuth => User != null || Password != null;

		[DataMember(Name = "Id")]
		public string Id { get; private set; }

		[DataMember(Name = "Interval"), Combiner(typeof(Union<>))]
		public IList<IntervalConfig> Intervals  { get; private set; }

		[DataMember(Name = "zones")]
		public string Zones { get; private set; }
	}
}
