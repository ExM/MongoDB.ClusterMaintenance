using ShardEqualizer.Models;

namespace ShardEqualizer.ShardSizeEqualizing
{
	public partial class ShardSizeEqualizer
	{
		public class Zone
		{
			private Bound _left;
			public Bound Left
			{
				get => _left;
				set
				{
					_left = value;
					_left.RightZone = this;
				}
			}

			private Bound _right;
			public Bound Right
			{
				get => _right;
				set
				{
					_right = value;
					_right.LeftZone = this;
				}
			}

			public long InitialSize  { get; }

			public Zone(ShardIdentity main, TagRange tagRange, long size, long targetSize)
			{
				Main = main;
				TagRange = tagRange;
				InitialSize = size;
				CurrentSize = size;
				TargetSize = targetSize;
			}

			public ShardIdentity Main { get; }
			public TagRange TagRange { get; }

			public TagIdentity Tag => TagRange.Tag;

			public BsonBound Min => _left.Value;
			public BsonBound Max => _right.Value;

			public long CurrentSize { get; private set; }

			public long TargetSize { get; private set; }

			public long Delta => TargetSize - InitialSize;

			public long RequirePressure
			{
				get
				{
					var leftPressure = Left.RequireShiftSize < 0 ? -Left.RequireShiftSize : 0;
					var rightPressure = Right.RequireShiftSize > 0 ? Right.RequireShiftSize : 0;
					return leftPressure + rightPressure;
				}
			}

			public long CurrentPressure
			{
				get
				{
					var leftPressure = Left.ShiftSize < 0 ? -Left.ShiftSize : 0;
					var rightPressure = Right.ShiftSize > 0 ? Right.ShiftSize : 0;
					return leftPressure + rightPressure;
				}
			}

			public void SizeUp(long v)
			{
				CurrentSize += v;
			}

			public void SizeDown(long v)
			{
				CurrentSize -= v;
			}
		}
	}
}
