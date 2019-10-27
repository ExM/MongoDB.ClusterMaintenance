using System;

namespace MongoDB.ClusterMaintenance.ShardSizeEqualizing
{
	public partial class ShardSizeEqualizer
	{
		public event EventHandler<ChunkMovingArgs> OnMoveChunk = (sender, args) => { };

		private void onChunkMoving(Zone sourceZone, Zone targetZone, Bound movingBound, long chunkSize)
		{
			OnMoveChunk(this, new ChunkMovingArgs(sourceZone, targetZone, movingBound, chunkSize));
		}
		
		public class ChunkMovingArgs: EventArgs
		{
			public ChunkMovingArgs(Zone source, Zone target, Bound bound, long chunkSize)
			{
				Source = source;
				Target = target;
				Bound = bound;
				ChunkSize = chunkSize;
			}

			public Zone Source { get; private set; }
			public Zone Target { get; private set; }
			public Bound Bound { get; private set; }
			public long ChunkSize { get; private set; }
		}
	}
}