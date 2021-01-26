using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace ShardEqualizer
{
	public class ProgressReporter: IAsyncDisposable
	{
		public ProgressReporter(string title, long total, Func<long, string> valueRenderer)
		{
			_title = title;
			_total = total;
			_valueRenderer = valueRenderer;
			_sw = Stopwatch.StartNew();
		}

		public void UpdateTotal(long total)
		{
			Interlocked.Add(ref _total, total);
		}

		public IEnumerable<string> RenderLines()
		{
			var total = Interlocked.Read(ref _total);
			var completed = Interlocked.Read(ref _completed);
			var elapsed = _sw.Elapsed;
			TimeSpan left;


			if (total <= completed)
				left = TimeSpan.Zero;
			else if(completed == 0)
				left = TimeSpan.MaxValue;
			else
			{
				var leftPercent = (double) (total - completed) / completed;
				left = TimeSpan.FromSeconds(elapsed.TotalSeconds * leftPercent);
			}


			if (IsCompleted)
				return new[] {$"{_title} ... {_completeMessage}"};

			if(total == 0)
				return new[]
				{
					$"{_title} ...",
					$"# Elapsed: {elapsed:d\\.hh\\:mm\\:ss\\.f}"
				};

			return new[]
			{
				$"{_title} ...",
				$"# Progress: {_valueRenderer(completed)}/{_valueRenderer(total)} Elapsed: {elapsed:d\\.hh\\:mm\\:ss\\.f} Left: {left:d\\.hh\\:mm\\:ss\\.f}"
			};
		}

		public void Increment()
		{
			Interlocked.Increment(ref _completed);
		}

		public void Increment(long value)
		{
			Interlocked.Add(ref _completed, value);
		}

		public void UpdateCurrent(long current)
		{
			Interlocked.Exchange(ref _completed, current);
		}

		public void SetCompleteMessage(string message)
		{
			_completeMessage = message;
		}

		public bool IsCompleted => _completeMessage != null;

		private long _completed;
		private string _completeMessage;
		private readonly string _title;
		private long _total;
		private readonly Func<long, string> _valueRenderer;
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
