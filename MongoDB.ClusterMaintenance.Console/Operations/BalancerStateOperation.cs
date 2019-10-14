using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.ClusterMaintenance.Config;
using MongoDB.ClusterMaintenance.Models;
using MongoDB.ClusterMaintenance.WorkFlow;
using MongoDB.Driver;
using NLog;

namespace MongoDB.ClusterMaintenance.Operations
{
	public class BalancerStateOperation: IOperation
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger();
		
		private readonly IConfigDbRepositoryProvider _configDb;
		private readonly IReadOnlyList<Interval> _intervals;
		
		private IReadOnlyCollection<Shard> _shards;
		private int _totalUnMovedChunks = 0;

		public BalancerStateOperation(IConfigDbRepositoryProvider configDb, IReadOnlyList<Interval> intervals)
		{
			_intervals = intervals;
			_configDb = configDb;
		}
		
		private async Task loadShards(CancellationToken token)
		{
			_shards = await _configDb.Shards.GetAll();
		}
		
		private async Task<int> scanInterval(Interval interval, CancellationToken token)
		{
			var currentTags = new HashSet<TagIdentity>(interval.Zones);
			var tagRanges = await _configDb.Tags.Get(interval.Namespace);
			tagRanges = tagRanges.Where(_ => currentTags.Contains(_.Tag)).ToList();

			var intervalCount = 0;

			foreach (var tagRange in tagRanges)
			{
				var validShards = new HashSet<ShardIdentity>(_shards.Where(_ => _.Tags.Contains(tagRange.Tag)).Select(_ => _.Id));
					
				var chunks = await (await _configDb.Chunks.ByNamespace(interval.Namespace)
					.From(tagRange.Min).To(tagRange.Max).Find()).ToListAsync(token);

				var unMovedChunks = chunks.Where(_ => _.Jumbo != true && !validShards.Contains(_.Shard)).ToList();
				if (unMovedChunks.Count != 0)
				{
					var sourceShards = unMovedChunks.Select(_ => _.Shard).Distinct().Select(_ => $"'{_}'");
					_log.Info("  tag range '{0}' wait {1} chunks from {2} shards",
						tagRange.Tag, unMovedChunks.Count, string.Join(", ", sourceShards));
					if (unMovedChunks.Count <= 5)
					{
						_log.Info("  chunksIds: {0}",
							string.Join(", ", unMovedChunks.Select(_ => _.Id)));
					}

					intervalCount += unMovedChunks.Count;
				}
			}
			
			return intervalCount;
		}
		
		private ObservableTask scanIntervals(CancellationToken token)
		{
			return ObservableTask.WithParallels(
				_intervals.Where(_ => _.Selected).ToList(), 
				16, 
				scanInterval,
				intervalCounts => { _totalUnMovedChunks = intervalCounts.Sum(); },
				token);
		}
	
		public async Task Run(CancellationToken token)
		{
			var opList = new WorkList()
			{
				{ "Load user databases", new SingleWork(loadShards, () => $"found {_shards.Count} shards.")},
				{ "Scan intervals", new ObservableWork(scanIntervals, () => _totalUnMovedChunks == 0
					? "all chunks moved."
					: $"found {_totalUnMovedChunks} chunks is awaiting movement.")}
			};

			await opList.Apply(token);
		}
	}
}