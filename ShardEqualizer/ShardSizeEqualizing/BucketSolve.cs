using System;
using MongoDB.Driver;
using ShardEqualizer.Models;

namespace ShardEqualizer.ShardSizeEqualizing
{
	public class BucketSolve
	{
		private long _targetSize;

		public BucketSolve(Bucket source)
		{
			Shard = source.Shard;
			Collection = source.Collection;
			CurrentSize = source.CurrentSize;
			_targetSize = source.CurrentSize;

			Delta = 0;
		}

		public int? VariableIndex { get; set; }
		public LinearPolinomial VariableFunction { get; set; }

		public long TargetSize
		{
			get => _targetSize;
			set
			{
				_targetSize = value;
				Delta = value - CurrentSize;
			}
		}

		public ShardIdentity Shard { get; }
		public CollectionNamespace Collection { get; }

		public long CurrentSize { get; private set; }

		public long Delta { get; private set; }

		public long PartialTargetSize(double percent) =>
			CurrentSize + (long) Math.Round(percent * (TargetSize - CurrentSize));
	}
}
