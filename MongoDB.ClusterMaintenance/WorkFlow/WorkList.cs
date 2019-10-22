using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public class WorkList: System.Collections.IEnumerable
	{
		private readonly List<OpItem> _opList = new List<OpItem>();
		
		public void Add(string title, Action<CancellationToken> work)
		{
			_opList.Add(new OpItem()
			{
				Order = _opList.Count + 1,
				Title = title,
				Work = work
			});
		}
		
		public void Add(string title, IWork work)
		{
			_opList.Add(new OpItem()
			{
				Order = _opList.Count + 1,
				Title = title,
				Work = work
			});
		}
		
		public void Add(string title, WorkList workList)
		{
			_opList.Add(new OpItem()
			{
				Order = _opList.Count + 1,
				Title = title,
				Work = workList
			});
		}

		public Task Apply(CancellationToken token)
		{
			return apply(string.Empty, token);
		}

		private async Task apply(string prefix, CancellationToken token)
		{
			var itemPrefix = prefix + "- ";
			
			foreach (var item in _opList)
			{
				Console.Write($"{prefix}{item.Order}/{_opList.Count} {item.Title}");
				switch (item.Work)
				{
					case WorkList innerList:
						Console.WriteLine();
						await innerList.apply(itemPrefix, token);
						break;
					
					case IWork work:
						Console.Write(" ... ");
						await work.Apply(token);
						break;
					
					case Action<CancellationToken> action:
						Console.WriteLine();
						action(token);
						break;
				}
			}
		}

		public IEnumerator GetEnumerator()
		{
			throw new NotImplementedException();
		}

		private class OpItem
		{
			public int Order;
			public string Title;
			public object Work;
		}
	}
}