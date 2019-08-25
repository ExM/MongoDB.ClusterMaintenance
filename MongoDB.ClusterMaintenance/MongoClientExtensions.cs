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
		public static async Task<IList<string>> ListUserDatabases(this IMongoClient mongoClient, CancellationToken token)
		{
			var allDatabaseNames = await mongoClient.ListDatabaseNames().ToListAsync(token);
			return allDatabaseNames.Except(new[] {"admin", "config"}).ToList();
		}
		
		public static async Task<IList<CollectionNamespace>> ListUserCollections(this IMongoClient mongoClient, CancellationToken token)
		{
			var userDataBases = await mongoClient.ListUserDatabases(token);
			var progress = new Progress(userDataBases.Count);
			return await mongoClient.ListUserCollections(userDataBases, progress, token);
		}
		
		public static async Task<IList<CollectionNamespace>> ListUserCollections(this IMongoClient mongoClient, IList<string> userDataBases, Progress progress, CancellationToken token)
		{
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
			
			return (await userDataBases.ParallelsAsync(listCollectionNames, 32, token)).SelectMany(_ => _).ToList();
		}
	}

	public class ObservableWork
	{
		public ObservableWork(Progress progress, Task work)
		{
			Progress = progress;
			Work = work;
		}

		public Task Work { get; } 
		public Progress Progress { get; }
	}

	public class Progress
	{
		public long Completed { get; private set; } 
		public long Total { get; private set; }
		public TimeSpan Elapset { get; private set; }
		public TimeSpan Left { get; private set; }
		
		public Progress(long total)
		{
			_total = total;
			_sw.Restart();
		}
		
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
		
		private long _completed;
		private readonly long _total;
		private readonly Stopwatch _sw = new Stopwatch();
	}
}