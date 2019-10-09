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
		private readonly string _title;

		public ObservableWork(string title, Func<CancellationToken, ObservableTask> action, Func<string> doneMessageRenderer = null)
		{
			_action = action;
			_doneMessageRenderer = doneMessageRenderer;
			_title = title;
		}

		public virtual async Task Apply(int indent, string prefix, CancellationToken token)
		{
			Console.Write(indent.ToIndent());
			Console.Write(prefix);
			Console.Write(_title);
			Console.Write(" ... ");
			var work = _action(token);

			var progress = work.Progress;

			var frame = new ConsoleFrame(builder =>
			{
				progress.Refresh();
				builder.AppendLine("");
				builder.AppendLine(
					$"# Progress: {progress.Completed}/{progress.Total} Elapsed: {progress.Elapset} Left: {progress.Left}");
			});

			var cts = new CancellationTokenSource();

			var cancelProgressLoop = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, token).Token;
			
			var progressTask = Task.Factory.StartNew(() => showProgressLoop(frame, cancelProgressLoop), TaskCreationOptions.LongRunning);

			try
			{
				await work.Work;
			}
			finally
			{
				cts.Cancel();
				await progressTask;
				frame.Clear();
			}

			var doneMessage = _doneMessageRenderer == null ? "done" : _doneMessageRenderer();
			Console.WriteLine(doneMessage);
		}
		
		private async Task showProgressLoop(ConsoleFrame frame, CancellationToken token)
		{
			while (!token.IsCancellationRequested)
			{
				frame.Refresh();

				try
				{
					await Task.Delay(250, token);
				}
				catch (TaskCanceledException)
				{
					return;
				}
			}
		}
	}
}