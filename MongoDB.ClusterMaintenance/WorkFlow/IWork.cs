using System.Threading;
using System.Threading.Tasks;

namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public interface IWork
	{
		Task Apply(string prefix, CancellationToken token);
	}
}