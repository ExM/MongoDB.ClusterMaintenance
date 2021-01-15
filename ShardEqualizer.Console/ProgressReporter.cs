using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace ShardEqualizer
{
	public class ProgressReporter: IAsyncDisposable
	{
		public long Completed { get; private set; }
		public long Total { get; private set; }
		public TimeSpan Elapsed { get; private set; }
		public TimeSpan Left { get; private set; }

		public ProgressReporter(string title, long total)
		{
			_title = title;
			_total = total;
			_sw = Stopwatch.StartNew();
		}

		public void Refresh()
		{
			Total = _total;
			Completed = Interlocked.Read(ref _completed);
			Elapsed = _sw.Elapsed;

			if (Total <= Completed)
				Left = TimeSpan.Zero;
			else if(Completed == 0)
				Left = TimeSpan.MaxValue;
			else
			{
				var leftPercent = (double) (Total - Completed) / Completed;
				Left = TimeSpan.FromSeconds(Elapsed.TotalSeconds * leftPercent);
			}
		}

		public IEnumerable<string> RenderLines()
		{
			Refresh();
			if (IsCompleted)
				return new[] {$"{_title} ... {_completeMessage}"};

			if(Total == 0)
				return new[]
				{
					$"{_title} ...",
					$"# Elapsed: {Elapsed:d\\.hh\\:mm\\:ss\\.f}"
				};

			return new[]
			{
				$"{_title} ...",
				$"# Progress: {Completed}/{Total} Elapsed: {Elapsed:d\\.hh\\:mm\\:ss\\.f} Left: {Left:d\\.hh\\:mm\\:ss\\.f}"
			};
		}

		public void Increment()
		{
			Interlocked.Increment(ref _completed);
		}

		public void SetCompleteMessage(string message)
		{
			_completeMessage = message;
		}

		public bool IsCompleted => _completeMessage != null;

		private long _completed;
		private string _completeMessage;
		private readonly string _title;
		private readonly long _total;
		private readonly Stopwatch _sw;

		private readonly TaskCompletionSource<object> _tcs = new TaskCompletionSource<object>();

		public void CompleteRendering()
		{
			_tcs.TrySetResult(null);
		}

		public async ValueTask DisposeAsync()
		{
			if (!IsCompleted)
				SetCompleteMessage("done.");

			await _tcs.Task;
		}
	}
}
