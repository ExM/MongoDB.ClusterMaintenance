using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.ClusterMaintenance.UI;

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
				doWork(cts.Token).Wait(cts.Token);
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

		private static async Task doWork(CancellationToken token)
		{
			Console.Write(">>>");
			int round = 0;

			var cf = new ConsoleFrame(b =>
			{
				b.AppendLine((500 - round).ToString());
				b.AppendLine(round.ToString());
			});
		
			while (!token.IsCancellationRequested)
			{
				await Task.Delay(1000, token);
				round++;
				
				cf.Refresh();
			}
		}
	}
}