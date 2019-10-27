using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.ClusterMaintenance.UI;

namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public class ObservableWork: IWork
	{
		private readonly Func<CancellationToken, ObservableTask> _action;
		private readonly Func<string> _doneMessageRenderer;

		public ObservableWork(Func<CancellationToken, ObservableTask> action, Func<string> doneMessageRenderer = null)
		{
			_action = action;
			_doneMessageRenderer = doneMessageRenderer;
		}

		public async Task Apply(CancellationToken token)
		{
			var work = _action(token);

			var progress = work.Progress;

			var cts = new CancellationTokenSource();

			var cancelProgressLoop = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, token).Token;
			
			var progressTask = Task.Factory.StartNew(() => showProgressLoop(progress, cancelProgressLoop), TaskCreationOptions.LongRunning);

			await work.Task;
			cts.Cancel();
			
			await progressTask;

			Console.WriteLine(_doneMessageRenderer == null ? "done" : _doneMessageRenderer());
		}
		
		private async Task showProgressLoop(Progress progress, CancellationToken token)
		{
			var frame = new ConsoleBookmark();
			while (!token.IsCancellationRequested)
			{
				progress.Refresh();
				frame.ClearAndRender(new []
				{
					"",
					$"# Progress: {progress.Completed}/{progress.Total} Elapsed: {progress.Elapsed:d\\.hh\\:mm\\:ss\\.f} Left: {progress.Left:d\\.hh\\:mm\\:ss\\.f}"
				});

				try
				{
					await Task.Delay(200, token);
				}
				catch (TaskCanceledException)
				{
					break;
				}
			}
			
			frame.Clear();
		}
	}
}