using System;
using System.Collections.Generic;
using System.Linq;

namespace MongoDB.ClusterMaintenance.UI
{
	public class ConsoleFrame: IFrameBuilder
	{
		private readonly Action<IFrameBuilder> _frameRenderer;
		private readonly int _startTop;
		private readonly int _startLeft;
		
		private readonly List<int> _renderedLines = new List<int>();
		
		private readonly object _sync = new object();
		
		public ConsoleFrame(Action<IFrameBuilder> frameRenderer)
		{
			_frameRenderer = frameRenderer;
			_startTop = Console.CursorTop;
			_startLeft = Console.CursorLeft;
		}

		public void Clear()
		{
			lock (_sync)
				innerClear();
		}

		private void innerClear()
		{
			Console.SetCursorPosition(_startLeft, _startTop);

			if (_renderedLines.Count == 0)
				return;

			Console.Write(new string(' ', _renderedLines.First()));
			foreach (var lineLength in _renderedLines.Skip(1))
			{
				Console.WriteLine();
				Console.Write(new string(' ', lineLength));
			}

			_renderedLines.Clear();

			Console.SetCursorPosition(_startLeft, _startTop);
		}

		public void Refresh()
		{
			lock (_sync)
			{
				innerClear();
				_frameRenderer(this);
			}
		}

		void IFrameBuilder.AppendLine(string text)
		{
			if(_renderedLines.Count != 0)
				Console.WriteLine();
			Console.Write(text);
			_renderedLines.Add(text.Length);
		}
	}
}