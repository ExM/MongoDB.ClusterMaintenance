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
				var opList = new OperationList("Base list operation")
				{
					new SingleWork("Single work 1", singleWork, () => $"Finished."),
					new ObservableWork("Observable work 1", observableWork, () => $"Finished."),
					new OperationList("Inner list L2")
					{
						new ObservableWork("Observable work 2.1", observableWork),
						
						new OperationList("Inner list L3")
						{
							new ObservableWork("Observable work 3.1", observableWork),
							new ObservableWork("Observable work 3.2", observableWork),
							new ObservableWork("Observable work 3.3", observableWork),
							new ObservableWork("Observable work 3.4", observableWork),
							new ObservableWork("Observable work 3.5", observableWork),
						},
						new ObservableWork("Observable work 2.2", observableWork),
					},
					new ObservableWork("Observable work 2", observableWork),
				};

				opList.Apply(0, "", cts.Token).Wait(cts.Token);
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
			var innerTasks = Enumerable.Range(0, 1000).ToList();
			
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