using System.Threading;
using System.Threading.Tasks;

namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public interface IWork
	{
		Task Apply(int indent, string prefix, CancellationToken token);
	}
}