using System;
using System.Collections.Generic;
using System.Threading;
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

		public static ObservableTask WithParallels<TItem, TResult>(
			IList<TItem> items,
			int maxParallelizm,
			Func<TItem, CancellationToken, Task<TResult>> actionTask,
			Action<IReadOnlyList<TResult>> saveResult,
			CancellationToken token)
		{
			var progress = new Progress(items.Count);
			
			async Task<TResult> singleWork(TItem item, CancellationToken t)
			{
				try
				{
					return await actionTask(item, t);
				}
				finally
				{
					progress.Increment();
				}
			}
			
			async Task saveResultWork()
			{
				saveResult(await items.ParallelsAsync(singleWork, maxParallelizm, token));
			}
			
			return new ObservableTask(progress, saveResultWork());
		}
	}
}