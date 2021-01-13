using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ShardEqualizer.WorkFlow
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
			IReadOnlyCollection<TItem> items,
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

		public static ObservableTask WithParallels<TItem>(
			IReadOnlyCollection<TItem> items,
			int maxParallelizm,
			Func<TItem, CancellationToken, Task> actionTask,
			CancellationToken token)
		{
			var progress = new Progress(items.Count);

			async Task singleWork(TItem item, CancellationToken t)
			{
				try
				{
					await actionTask(item, t);
				}
				finally
				{
					progress.Increment();
				}
			}

			return new ObservableTask(progress, items.ParallelsAsync(singleWork, maxParallelizm, token));
		}
	}
}
