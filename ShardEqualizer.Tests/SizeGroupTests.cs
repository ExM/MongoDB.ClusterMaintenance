using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using ShardEqualizer.Models;
using ShardEqualizer.Reporting;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer
{
	[TestFixture]
	public class SizeGroupTests
	{
		[Test]
		public void Demo()
		{
			var report = new TestReport();

			report.Append(new CollectionStatistics() {Primary = new ShardIdentity("A"), Sharded  = false, Size = 100}, null);
			report.Append(new CollectionStatistics(){Primary = new ShardIdentity("A"), Sharded  = false, Size = 100}, false);
			report.Append(new CollectionStatistics(){Primary = new ShardIdentity("A"), Sharded  = false, Size = 100}, true);
			report.Append(new CollectionStatistics(){Primary = new ShardIdentity("A"), Sharded  = false, Size = 100}, true);

			report.Append(new CollectionStatistics(){Sharded  = true, Shards = new Dictionary<ShardIdentity, ShardCollectionStatistics>()
			{
				{new ShardIdentity("A"), new ShardCollectionStatistics() { Size = 100} },
				{new ShardIdentity("B"), new ShardCollectionStatistics() { Size = 10} },
			}}, null);

			report.Append(new CollectionStatistics(){Sharded  = true, Shards = new Dictionary<ShardIdentity, ShardCollectionStatistics>()
			{
				{new ShardIdentity("A"), new ShardCollectionStatistics() { Size = 100} },
				{new ShardIdentity("B"), new ShardCollectionStatistics() { Size = 10} },
			}}, false);

			report.Append(new CollectionStatistics(){Sharded  = true, Shards = new Dictionary<ShardIdentity, ShardCollectionStatistics>()
			{
				{new ShardIdentity("A"), new ShardCollectionStatistics() { Size = 100} },
				{new ShardIdentity("B"), new ShardCollectionStatistics() { Size = 10} },
			}}, true);

			report.Append(new CollectionStatistics(){Sharded  = true, Shards = new Dictionary<ShardIdentity, ShardCollectionStatistics>()
			{
				{new ShardIdentity("A"), new ShardCollectionStatistics() { Size = 100} },
				{new ShardIdentity("B"), new ShardCollectionStatistics() { Size = 10} },
			}}, true);

			report.Append(new CollectionStatistics(){Primary = new ShardIdentity("B"), Sharded  = false, Size = 100}, null);
			report.Append(new CollectionStatistics(){Primary = new ShardIdentity("B"), Sharded  = false, Size = 100}, false);
			report.Append(new CollectionStatistics(){Primary = new ShardIdentity("B"), Sharded  = false, Size = 100}, true);
			report.Append(new CollectionStatistics(){Primary = new ShardIdentity("B"), Sharded  = false, Size = 100}, true);

			report.Render(new [] { new ColumnDescription(DataType.Total, SizeType.DataSize, false)});


			Assert.AreEqual(800, report.FirstColumn["A"]);
			Assert.AreEqual(440, report.FirstColumn["B"]);
		}

		public class TestReport: BaseReport
		{
			public Dictionary<string, long> FirstColumn = new Dictionary<string, long>();

			public TestReport() : base(new SizeRenderer("", ScaleSuffix.None))
			{
			}

			protected override void AppendRow(StringBuilder sb, string rowTitle, params long?[] cells)
			{
				FirstColumn.Add(rowTitle, cells[0].Value);
			}

			protected override void AppendHeader(StringBuilder sb, IEnumerable<string> cells)
			{
			}
		}
	}
}
