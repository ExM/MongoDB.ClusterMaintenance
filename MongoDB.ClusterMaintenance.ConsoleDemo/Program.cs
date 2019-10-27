using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.ClusterMaintenance.MongoCommands;
using MongoDB.ClusterMaintenance.UI;
using MongoDB.ClusterMaintenance.WorkFlow;

namespace MongoDB.ClusterMaintenance.ConsoleDemo
{
	internal class Program
	{
		public static int Main(string[] args)
		{
			var cts = new CancellationTokenSource();

			Console.CancelKeyPress += (sender, eventArgs) =>
			{
				cts.Cancel();
				eventArgs.Cancel = true;
				Console.WriteLine("ctrl+C");
			};
			try
			{
				var opList = new WorkList()
				{
					{"Single work 1", new SingleWork(singleWork, () => $"Finished.")},
					{"Observable work 1", new ObservableWork(observableWork, () => $"Finished.")},
					{"Inner list L2", new WorkList() {
						{"Observable work 2.1", new ObservableWork(observableWork)},
						{"Inner list L3", new WorkList()
						{
							{ "Observable work 3.1", new ObservableWork(observableWork)},
							{ "Observable work 3.2", new ObservableWork(observableWork)},
							{ "Observable work 3.3", new ObservableWork(observableWork)},
							{ "Observable work 3.4", new ObservableWork(observableWork)},
							{ "Observable work 3.5", new ObservableWork(observableWork)},
						}},
						{"Observable work 2.2", new ObservableWork(observableWork)},
					}},
					{"Observable work 2", new ObservableWork(observableWork)},
				};

				opList.Apply(cts.Token).Wait(cts.Token);
			}
			catch (OperationCanceledException)
			{
			}
			catch (Exception ex)
			{
				Console.WriteLine("Unexpected exception:");
				Console.WriteLine(ex);
				return -1;
			}
			
			return 0;
		}

		private static async Task singleWork(CancellationToken token)
		{
			await Task.Delay(TimeSpan.FromSeconds(2), token);
		}
		
		private static ObservableTask observableWork(CancellationToken token)
		{
			var innerTasks = Enumerable.Range(0, 500).ToList();
			
			var rnd = new Random();
			
			var progress = new Progress(innerTasks.Count);

			async Task<int> listCollectionNames(int input, CancellationToken t)
			{
				try
				{
					await Task.Delay(TimeSpan.FromMilliseconds(rnd.Next(5, 100)), t);
					return 0;
				}
				finally
				{
					progress.Increment();
				}
			}
			
			async Task work()
			{
				await innerTasks.ParallelsAsync(listCollectionNames, 10, token);
			}
			
			return new ObservableTask(progress, work());
		}
	}
}