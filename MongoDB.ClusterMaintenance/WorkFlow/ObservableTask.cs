using System.Threading.Tasks;

namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public class ObservableTask
	{
		public ObservableTask(Progress progress, Task work)
		{
			Progress = progress;
			Work = work;
		}

		public Task Work { get; } 
		public Progress Progress { get; }
	}
}