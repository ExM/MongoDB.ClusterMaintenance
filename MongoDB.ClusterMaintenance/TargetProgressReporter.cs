using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.ClusterMaintenance.UI;
using NLog;

namespace MongoDB.ClusterMaintenance
{
	public class TargetProgressReporter
	{
		private readonly long _target;
		private readonly Func<long, string> _valueRenderer;
		private readonly long _source;
		private readonly CancellationTokenSource _cts;
		private readonly Task _loop;
		private readonly Stopwatch _sw;

		private readonly object _sync = new object();
		private readonly ConsoleBookmark _consoleBookmark= new ConsoleBookmark();
		private long _current;
		private bool _outerStateRendered = false;
		private readonly List<string> _outerState = new List<string>();

		public TargetProgressReporter(long source, long target, Func<long, string> valueRenderer = null)
		{
			_target = target;
			_valueRenderer = valueRenderer ?? defaultRenderer;
			_source = source;
			_current = source;
			
			_cts = new CancellationTokenSource();
			_sw = Stopwatch.StartNew();
			_loop = showProgressLoop();
		}

		private static string defaultRenderer(long value)
		{
			return value.ToString(CultureInfo.InvariantCulture);
		}

		public void Update(long current)
		{
			lock (_sync)
				_current = current;
		}
		
		private async Task showProgressLoop()
		{
			while (true)
			{
				lock (_sync)
					showProgress();
					
				try
				{
					await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);
				}
				catch (TaskCanceledException)
				{
					break;
				}
			}
			
			lock (_sync)
				_consoleBookmark.Clear();
		}

		private void showProgress()
		{
			var elapsed = _sw.Elapsed;
			var percent = (double) (_source - _current) / (_source - _target);
			var s = percent <= 0 ? 0 : (1 - percent) / percent;

			var eta = TimeSpan.FromSeconds(elapsed.TotalSeconds * s);

			_consoleBookmark.Clear();
			_consoleBookmark.Render("");
			_consoleBookmark.Render(
				$"Progress {_valueRenderer(_current)}/{_valueRenderer(_target)} Elapsed: {elapsed:d\\.hh\\:mm\\:ss\\.f} ETA: {eta:d\\.hh\\:mm\\:ss\\.f}");

			foreach (var line in _outerState)
			{
				_consoleBookmark.Render(line);
			}

			_outerStateRendered = true;
		}

		public async Task Stop()
		{
			_cts.Cancel();
			await _loop;
		}

		public void TryRender(Func<string[]> linesRenderer)
		{
			lock (_sync)
			{
				if (!_outerStateRendered)
					return;

				_outerStateRendered = false;

				_outerState.Clear();

				foreach (var line in linesRenderer())
					_outerState.Add(line);
			}
		}
	}
}