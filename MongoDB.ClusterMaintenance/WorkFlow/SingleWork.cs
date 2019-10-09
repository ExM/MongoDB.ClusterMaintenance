using System;
using System.Threading;
using System.Threading.Tasks;

namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public class SingleWork: IWork
	{
		private readonly Func<CancellationToken, Task> _action;
		private readonly Func<string> _doneMessageRenderer;
		private readonly string _title;

		public SingleWork(string title, Func<CancellationToken, Task> action, Func<string> doneMessageRenderer = null)
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
			await _action(token);

			var doneMessage = _doneMessageRenderer == null ? "done" : _doneMessageRenderer();
			Console.WriteLine(doneMessage);
		}
	}
}