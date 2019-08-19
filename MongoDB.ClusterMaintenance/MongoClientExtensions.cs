using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace MongoDB.ClusterMaintenance
{
	public static class MongoClientExtensions
	{
		public static Task<IList<CollectionNamespace>> ListUserCollections(this IMongoClient mongoClient, CancellationToken token)
		{
			return mongoClient.ListUserCollections(new Progress(), token);
		}
		
		public static async Task<IList<CollectionNamespace>> ListUserCollections(this IMongoClient mongoClient, Progress progress, CancellationToken token)
		{
			var allDatabaseNames = await mongoClient.ListDatabaseNames().ToListAsync(token);
			var userDataBases = allDatabaseNames.Except(new[] {"admin", "config"}).ToList();
			progress.Start(userDataBases.Count);
			async Task<IEnumerable<CollectionNamespace>> listCollectionNames(string dbName, CancellationToken t)
			{
				try
				{
					var db = mongoClient.GetDatabase(dbName);
					var collNames = await db.ListCollectionNames().ToListAsync(t);
					return collNames.Select(_ => new CollectionNamespace(dbName, _));
				}
				finally
				{
					progress.Increment();
				}
			}
			
			return (await userDataBases.ParallelsAsync(listCollectionNames, 2, token)).SelectMany(_ => _).ToList();
		}
	}

	public class Progress
	{
		public long Completed { get; private set; } 
		public long Total { get; private set; }
		public TimeSpan Elapset { get; private set; }
		public TimeSpan Left { get; private set; }
		
		public void Refresh()
		{
			Total = _total;
			Completed = Interlocked.Read(ref _completed);
			Elapset = _sw.Elapsed;

			if (Total <= Completed)
				Left = TimeSpan.Zero;
			else if(Completed == 0)
				Left = TimeSpan.MaxValue;
			else
			{
				var leftPercent = (double) (Total - Completed) / Completed;
				Left = TimeSpan.FromSeconds(Elapset.TotalSeconds * leftPercent);
			}
		}

		public void Increment()
		{
			Interlocked.Increment(ref _completed);
		}

		public void Start(long total)
		{
			_total = total;
			_sw.Restart();
			Interlocked.MemoryBarrier();
		}
		
		private long _completed;
		private long _total;
		private readonly Stopwatch _sw = new Stopwatch();
	}
}