using System;
using ShardEqualizer.ShortModels;

namespace ShardEqualizer.Reporting
{
	public class SizeDetails
	{
		public long DataActual;
		public long DataStorage;
		public long Index;
		public long AllStorage => DataStorage + Index;

		public void Add(ShardCollectionStatistics collStats)
		{
			DataActual += collStats.Size;
			DataStorage += collStats.StorageSize;
			Index += collStats.TotalIndexSize;
		}

		public long BySizeType(SizeType sizeType)
		{
			switch (sizeType)
			{
				case SizeType.DataSize: return DataActual;
				case SizeType.DataStorage: return DataStorage;
				case SizeType.IndexSize: return Index;
				case SizeType.TotalStorage: return AllStorage;
				default:
					throw new ArgumentOutOfRangeException(nameof(sizeType), sizeType, null);
			}
		}
	}
}
