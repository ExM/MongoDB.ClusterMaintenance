using System.Collections.Generic;
using System.Linq;
using System.Text;
using ShardEqualizer.Models;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer.Reporting
{
	public abstract class BaseReport
	{
		protected readonly SizeRenderer SizeRenderer;

		private readonly Dictionary<ShardIdentity, ShardRow> _rows = new Dictionary<ShardIdentity, ShardRow>();

		protected BaseReport(SizeRenderer sizeRenderer)
		{
			SizeRenderer = sizeRenderer;
		}

		public void Append(CollectionStatistics collStats, bool? correctionMode)
		{
			if (collStats.Sharded)
			{
				foreach (var shardCollStats in collStats.Shards)
				{
					var row = ensureRow(shardCollStats.Key);

					switch (correctionMode)
					{
						case false:
							row.Fixed.Add(shardCollStats.Value);
							break;
						case true:
							row.Adjustable.Add(shardCollStats.Value);
							break;
						default:
							row.UnManaged.Add(shardCollStats.Value);
							break;
					}
				}
			}
			else
			{
				ensureRow(collStats.Primary.Value).UnSharded.Add(collStats);
			}
		}

		private ShardRow ensureRow(ShardIdentity shardName)
		{
			if (_rows.ContainsKey(shardName))
				return _rows[shardName];
			var row = new ShardRow(this);
			_rows.Add(shardName, row);
			return row;
		}

		public StringBuilder Render(IReadOnlyList<ColumnDescription> collDescriptions)
		{
			var totalRows = _rows.Count;
			var colls = collDescriptions.Select(cd => cd.CreateColumn(totalRows)).ToArray();

			var currentRow = 0;
			foreach (var p in _rows.OrderBy(_ => _.Key))
			{
				foreach (var coll in colls)
					coll.SetRow(currentRow, p.Value);

				currentRow++;
			}

			foreach (var coll in colls)
				coll.CalcTotal();

			var sb = new StringBuilder();

			AppendHeader(sb, new [] {"Shards "}.Concat(collDescriptions.Select(_ => _.Header())));

			currentRow = 0;
			foreach (var p in _rows.OrderBy(_ => _.Key))
			{
				AppendRow(sb, (string) p.Key, colls.Select(r => r.GetRow(currentRow)).ToArray());
				currentRow++;
			}

			AppendRow(sb, "<total>", colls.Select(r => r.Total).ToArray());
			AppendRow(sb, "<average>", colls.Select(r => r.Average).ToArray());

			return sb;
		}

		protected abstract void AppendRow(StringBuilder sb, string rowTitle, params long?[] cells);

		protected abstract void AppendHeader(StringBuilder sb, IEnumerable<string> cells);
	}

	/*

Header Code FullName

DSize   z DataSize
DStore  s DataStorage
Index	i IndexSize
TStore  t TotalStorage = dt + is

rows:
* ByShard
* Total
* Average

Delta 	d Delta

UnShrd	u Unsharded
UnMan	m Unmanaged
Fixed	f Fixed
Adj		a Adjustable
Sharded s Sharded = um + fx + aj
Total	t Total = us + um + fx + aj

	 */
}
