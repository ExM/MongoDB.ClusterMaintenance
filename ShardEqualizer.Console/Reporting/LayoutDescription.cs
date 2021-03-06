using System;
using System.Collections.Generic;
using System.Linq;
using ShardEqualizer.Config;

namespace ShardEqualizer.Reporting
{
	public class LayoutDescription
	{
		public string Title { get; }
		
		public IReadOnlyList<ColumnDescription> Columns { get; }
		
		public static LayoutDescription Default => new LayoutDescription(LayoutConfig.Default);

		public LayoutDescription(LayoutConfig config)
		{
			Title = config.Title;

			Columns = config.Columns
				.Split(new char[] {' '}, StringSplitOptions.RemoveEmptyEntries)
				.Select(ColumnDescription.Parse)
				.ToList();
		}
	}
}