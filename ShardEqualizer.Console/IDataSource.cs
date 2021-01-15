using System.Threading;
using System.Threading.Tasks;

namespace ShardEqualizer
{
	public interface IDataSource<T>
	{
		Task<T> Get(CancellationToken token);
	}
}