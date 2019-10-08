using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public class OperationList: IWork, System.Collections.IEnumerable
	{
		private readonly string _title;
		
		private readonly List<IWork> _opList = new List<IWork>();

		public OperationList(string title = null)
		{
			_title = title;
		}

		public void Add(IWork operation)
		{
			_opList.Add(operation);
		}

		public async Task Apply(string prefix, CancellationToken token)
		{
			Console.Write(prefix);
			Console.WriteLine(_title ?? "");

			foreach (var item in _opList.Select((operation, order) => new {operation, order}))
			{
				await item.operation.Apply($"{item.order + 1}/{_opList.Count} ", token);
			}
		}

		public IEnumerator GetEnumerator()
		{
			throw new NotImplementedException();
		}
	}
}