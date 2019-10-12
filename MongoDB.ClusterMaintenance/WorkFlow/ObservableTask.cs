using System.Threading.Tasks;

namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public class ObservableTask
	{
		public ObservableTask(Progress progress, Task task)
		{
			Progress = progress;
			Task = task;
		}

		public Task Task { get; } 
		public Progress Progress { get; }
	}
}