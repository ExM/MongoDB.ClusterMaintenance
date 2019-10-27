using System;
using System.Diagnostics;
using System.Threading;

namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public class Progress
	{
		public long Completed { get; private set; } 
		public long Total { get; private set; }
		public TimeSpan Elapsed { get; private set; }
		public TimeSpan Left { get; private set; }
		
		public Progress(long total)
		{
			_total = total;
			_sw.Restart();
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

		public void Increment()
		{
			Interlocked.Increment(ref _completed);
		}
		
		private long _completed;
		private readonly long _total;
		private readonly Stopwatch _sw = new Stopwatch();
	}
}