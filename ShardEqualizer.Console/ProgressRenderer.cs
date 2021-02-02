using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ShardEqualizer.UI;

namespace ShardEqualizer
{
	public class ProgressRenderer: IAsyncDisposable
	{
		private readonly CancellationTokenSource _cts = new CancellationTokenSource();
		private readonly Task _renderTask;
		private readonly object _sync = new object();
		private readonly object _syncRender = new object();
		private readonly List<ProgressReporter> _reporters = new List<ProgressReporter>();
		private readonly List<string> _logLines = new List<string>();
		private IConsoleBookmark _frame;

		public ProgressRenderer()
		{
			_renderTask = Task.Factory.StartNew(() => showProgressLoop(_cts.Token), TaskCreationOptions.LongRunning);
		}

		private async Task showProgressLoop(CancellationToken token)
		{
			while (!token.IsCancellationRequested)
			{
				lock (_syncRender)
					renderReporterList();

				try
				{
					await Task.Delay(200, token); //TODO force render with complete
				}
				catch (TaskCanceledException)
				{
					break;
				}
			}
		}

		public void Flush()
		{
			lock (_syncRender)
				renderReporterList();
		}

		private void renderReporterList()
		{
			var completed = new List<ProgressReporter>();
			List<ProgressReporter> inProgress;
			List<string> copyLogLines;
			lock (_sync)
			{
				for (var i = _reporters.Count - 1; i >= 0; i--)
				{
					var reporter = _reporters[i];
					if (reporter.IsCompleted)
					{
						completed.Add(reporter);
						_reporters.RemoveAt(i);
					}
				}

				inProgress = _reporters.ToList();

				copyLogLines = _logLines.ToList();
				_logLines.Clear();
			}

			var completedLines = copyLogLines.Concat(completed.SelectMany(_ => _.RenderLines())).ToList();
			var inProgressLines = inProgress.SelectMany(_ => _.RenderLines()).ToList();

			if (_frame == null)
			{
				if (completedLines.Any())
				{
					foreach (var line in completedLines)
						Console.WriteLine(line);

					if (inProgressLines.Any())
					{
						_frame = createBookmark();
						foreach (var line in inProgressLines)
							_frame.Render(line);
					}
				}
				else
				{
					if (inProgressLines.Any())
					{
						_frame = createBookmark();
						foreach (var line in inProgressLines)
							_frame.Render(line);
					}
				}

			}
			else
			{
				if (completedLines.Any())
				{
					_frame.Clear();
					foreach (var line in completedLines)
						Console.WriteLine(line);

					if (inProgressLines.Any())
					{
						_frame = createBookmark();
						foreach (var line in inProgressLines)
							_frame.Render(line);
					}
					else
					{
						_frame = null;
					}
				}
				else
				{
					_frame.Clear();

					if (inProgressLines.Any())
					{
						foreach (var line in inProgressLines)
							_frame.Render(line);
					}
					else
					{
						_frame = null;
					}
				}
			}

			foreach (var reporter in completed)
				reporter.CompleteRendering();
		}

		public ProgressReporter Start(string title, long total = 0, Func<long, string> valueRenderer = null)
		{
			var reporter = new ProgressReporter(title, total, valueRenderer ?? defaultRenderer);

			lock (_sync)
				_reporters.Add(reporter);

			return reporter;
		}

		private string defaultRenderer(long v)
		{
			return v.ToString();
		}

		public void WriteLine(string line = "")
		{
			lock (_sync)
				_logLines.Add(line);
		}

		public async ValueTask DisposeAsync()
		{
			Flush();
			_cts.Cancel();
			await _renderTask;
		}

		private IConsoleBookmark createBookmark()
		{
			return new ConsoleBookmark(); //TODO configure
			//return new NonInteractiveConsole();
		}

		public class NonInteractiveConsole : IConsoleBookmark
		{
			public NonInteractiveConsole()
			{
				Console.Write(">");
			}

			public void Clear()
			{
				Console.WriteLine("<");
			}

			public void Render(string line)
			{
				Console.WriteLine(line);
			}
		}
	}
}
