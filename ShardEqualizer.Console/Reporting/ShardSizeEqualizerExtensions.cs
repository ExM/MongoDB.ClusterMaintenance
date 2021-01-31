using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using ShardEqualizer.ShardSizeEqualizing;

namespace ShardEqualizer.Reporting
{
	public static class ShardSizeEqualizerExtensions
	{
		public static IList<string> RenderMovePlan(this ShardSizeEqualizer equalizer, ScaleSuffix scaleSuffix)
		{
			var scaleSuffixFactor = (double)scaleSuffix.Factor();

			var rowCount = equalizer.Zones.Count;

			string renderValue(long value) =>
				(value / scaleSuffixFactor).ToString("0.00", CultureInfo.InvariantCulture);

			ColumnAligner createCollumn(string header)
			{
				var coll = new ColumnAligner(rowCount + 1);
				coll.AppendRow(header, true);
				return coll;
			}

			var shardColumn = createCollumn("shard");
			var currentColumn = createCollumn("current");
			var targetColumn = createCollumn("target");
			var deltaColumn = createCollumn("delta");
			var pressureColumn = createCollumn("pressure");

			var leftAColumn = createCollumn("left accepted");
			var rightAColumn = createCollumn("right accepted");

			var zones = equalizer.Zones.OrderBy(_ => _.Main).ToList();

			var leftAcceptedRows = renderLeftAcceptedRows(zones, renderValue);
			var rightAcceptedRows = renderRightAcceptedRows(zones, renderValue);

			var index = 0;
			foreach (var zone in zones)
			{
				shardColumn.AppendRow(zone.Main.ToString(), true);
				currentColumn.AppendRow(renderValue(zone.InitialSize), false);
				targetColumn.AppendRow(renderValue(zone.TargetSize), false);
				deltaColumn.AppendRow(renderValue(zone.Delta), false);
				pressureColumn.AppendRow(renderValue(zone.RequirePressure), false);

				leftAColumn.AppendRow(leftAcceptedRows[index], false);
				rightAColumn.AppendRow(rightAcceptedRows[index], false);
				index++;
			}

			var resultRows = new List<string>(rowCount + 1);
			for (var i = 0; i <= rowCount; i++)
				resultRows.Add($"| {shardColumn[i]} | {currentColumn[i]} | {targetColumn[i]} | {deltaColumn[i]} | {pressureColumn[i]} | {leftAColumn[i]} | {rightAColumn[i]} |");

			return resultRows;
		}

		private static IList<string> renderLeftAcceptedRows(IReadOnlyCollection<ShardSizeEqualizer.Zone> zones, Func<long, string> renderValue)
		{
			var shardColumn = new ColumnAligner(zones.Count);
			var directionColumn = new ColumnAligner(zones.Count);
			var sizeColumn = new ColumnAligner(zones.Count);

			foreach (var zone in zones)
			{
				if (zone.Left.LeftZone == null)
				{
					shardColumn.AppendRow("", true);
					directionColumn.AppendRow("", true);
					sizeColumn.AppendRow("", false);
				}
				else
				{
					shardColumn.AppendRow(zone.Left.LeftZone.Main.ToString(), true);
					var shift = zone.Left.RequireShiftSize;
					var dirSymbol = "";
					if (shift > 0)
						dirSymbol = "-";
					else if (shift < 0)
					{
						dirSymbol = "+";
						shift = -shift;
					}

					directionColumn.AppendRow(dirSymbol, true);
					sizeColumn.AppendRow(renderValue(shift), false);
				}
			}

			var resultRows = new List<string>(zones.Count);
			for (var i = 0; i < zones.Count; i++)
				resultRows.Add($"{shardColumn[i]} {directionColumn[i]} {sizeColumn[i]}");

			return resultRows;
		}

		private static IList<string> renderRightAcceptedRows(IReadOnlyCollection<ShardSizeEqualizer.Zone> zones, Func<long, string> renderValue)
		{
			var shardColumn = new ColumnAligner(zones.Count);
			var directionColumn = new ColumnAligner(zones.Count);
			var sizeColumn = new ColumnAligner(zones.Count);

			foreach (var zone in zones)
			{
				if (zone.Right.RightZone == null)
				{
					shardColumn.AppendRow("", true);
					directionColumn.AppendRow("", true);
					sizeColumn.AppendRow("", false);
				}
				else
				{
					shardColumn.AppendRow(zone.Right.RightZone.Main.ToString(), true);
					var shift = zone.Right.RequireShiftSize;
					var dirSymbol = "";
					if (shift > 0)
						dirSymbol = "+";
					else if (shift < 0)
					{
						dirSymbol = "-";
						shift = -shift;
					}

					directionColumn.AppendRow(dirSymbol, true);
					sizeColumn.AppendRow(renderValue(shift), false);
				}
			}

			var resultRows = new List<string>(zones.Count);
			for (var i = 0; i < zones.Count; i++)
				resultRows.Add($"{shardColumn[i]} {directionColumn[i]} {sizeColumn[i]}");

			return resultRows;
		}

		private class ColumnAligner
		{
			private readonly List<string> _rows;
			private readonly List<bool> _leftAlign;
			private int _maxLenght = 0;

			public ColumnAligner(int rowCount = 4)
			{
				_rows = new List<string>(rowCount);
				_leftAlign = new List<bool>(rowCount);
			}

			public void AppendRow(string cell, bool leftAlign)
			{
				_rows.Add(cell);
				_leftAlign.Add(leftAlign);
				if (_maxLenght < cell.Length)
					_maxLenght = cell.Length;
			}

			public string this[int rowIndex] => render(rowIndex);

			private string render(int rowIndex)
			{
				var row = _rows[rowIndex];
				var p = _maxLenght - row.Length;
				if (p == 0)
					return row;

				return _leftAlign[rowIndex]
					? row + new string(' ', p)
					:new string(' ', p) + row;
			}
		}
	}
}
