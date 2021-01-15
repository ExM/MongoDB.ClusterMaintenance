using System;
using System.Collections.Generic;
using System.Linq;

namespace ShardEqualizer.UI
{
	public interface IConsoleBookmark
	{
		void Clear();
		void Render(string line);
	}

	public class ConsoleBookmark: IConsoleBookmark
	{
		private readonly int _startTop;
		private readonly int _startLeft;
		private readonly List<int> _renderedLines = new List<int>();

		public ConsoleBookmark()
		{
			_startTop = Console.CursorTop;
			_startLeft = Console.CursorLeft;
		}

		public void Clear()
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

		public void ClearAndRender(IEnumerable<string> lines)
		{
			Clear();

			foreach (var line in lines)
				Render(line);
		}

		public void Render(string line)
		{
			if(_renderedLines.Count != 0)
				Console.WriteLine();
			Console.Write(line);
			_renderedLines.Add(line.Length);
		}
	}
}
