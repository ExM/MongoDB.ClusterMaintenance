using System;
using System.Threading;
using System.Threading.Tasks;

namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public class SingleWork: IWork
	{
		private readonly Func<CancellationToken, Task> _voidAction;
		private readonly Func<CancellationToken, Task<string>> _messageAction;
		
		private readonly Func<string> _doneMessageRenderer;

		public SingleWork(Func<CancellationToken, Task> action, Func<string> doneMessageRenderer = null)
		{
			_voidAction = action;
			_doneMessageRenderer = doneMessageRenderer;
		}
		
		public SingleWork(Func<CancellationToken, Task<string>> action)
		{
			_messageAction = action;
		}

		public virtual async Task Apply(CancellationToken token)
		{
			if (_voidAction != null)
			{
				await _voidAction(token);
				Console.WriteLine(_doneMessageRenderer == null ? "done" : _doneMessageRenderer());
			}
			else if (_messageAction != null)
			{
				var message = await _messageAction(token);
				Console.WriteLine(message ?? "done");
			}
			else
			{
				throw new NotImplementedException("no action");
			}
		}
	}
}