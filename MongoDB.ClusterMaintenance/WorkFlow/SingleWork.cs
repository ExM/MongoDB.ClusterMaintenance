using System;
using System.Threading;
using System.Threading.Tasks;

namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public class SingleWork: IWork
	{
		private readonly Func<CancellationToken, Task> _action;
		private readonly Func<string> _doneMessageRenderer;

		public SingleWork(Func<CancellationToken, Task> action, Func<string> doneMessageRenderer = null)
		{
			_action = action;
			_doneMessageRenderer = doneMessageRenderer;
		}

		public virtual async Task Apply(CancellationToken token)
		{
			await _action(token);
			Console.WriteLine(_doneMessageRenderer == null ? "done" : _doneMessageRenderer());
		}
	}
}