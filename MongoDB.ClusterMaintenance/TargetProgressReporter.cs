using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace MongoDB.ClusterMaintenance
{
	public class TargetProgressReporter
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();
		private long _current;
		private readonly long _target;
		private readonly Func<long, string> _renderer;
		private readonly long _source;
		private readonly bool _forward;
		private readonly Action _onShowProgress;
		private readonly CancellationTokenSource _cts;
		private readonly Task _loop;
		private readonly Stopwatch _sw;

		public TargetProgressReporter(long source, long target, Func<long, string> renderer = null, Action onShowProgress = null)
		{
			_target = target;
			_renderer = renderer ?? defaultRenderer;
			_source = source;
			_current = source;
			_forward = _source < _target;
			
			_onShowProgress = onShowProgress;
			_cts = new CancellationTokenSource();
			_log.Info("Start from {0} to {1}", _renderer(_source), _renderer(_target));
			_sw = Stopwatch.StartNew();
			_loop = showProgressLoop();
		}

		private static string defaultRenderer(long value)
		{
			return value.ToString(CultureInfo.InvariantCulture);
		}

		public void Update(long current)
		{
			Interlocked.Exchange(ref _current, current);
		}
		
		private async Task showProgressLoop()
		{
			while (showProgress())
				await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);
		}

		private bool showProgress()
		{
			var copyCurrent = Interlocked.Read(ref _current);

			var elapsed = _sw.Elapsed;
			var percent = (double)(_source - copyCurrent) / (_source - _target);
			var s = percent <= 0 ? 0 : (1 - percent) / percent;

			var eta = TimeSpan.FromSeconds(elapsed.TotalSeconds * s);

			_log.Info("Progress {0}/{1} Elapsed: {2} ETA: {3}", _renderer(copyCurrent), _renderer(_target), elapsed, eta);

			_onShowProgress?.Invoke();
			
			return _forward ? copyCurrent < _target : _target < copyCurrent;
		}

		public async Task Stop()
		{
			_cts.Cancel();
			try
			{
				await _loop;
			}
			catch (TaskCanceledException)
			{
			}
			
			var copyCurrent = Interlocked.Read(ref _current);
			_log.Info("Done {0}", _renderer(copyCurrent));
			_onShowProgress?.Invoke();
		}
	}
}