using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ShardEqualizer.Models;

namespace ShardEqualizer
{
	public static class CollectionExtensions
	{
		public static List<T> CalcPercentiles<T>(this IEnumerable<T> values, List<double> bounds) where T: IComparable<T>
		{
			var orderedValues = values.OrderBy(_ => _).ToList();

			var result = new List<T>(bounds.Count);

			foreach (var bound in bounds)
			{
				if (bound <= 0)
					result.Add(orderedValues.First());
				else if(bound >= 1)
					result.Add(orderedValues.Last());
				else
				{
					var valueIndex = (int) Math.Ceiling(bound * (orderedValues.Count - 1));
					result.Add(orderedValues[valueIndex]);
				}
			}

			return result;
		}

		public static IEnumerable<List<T>> Split<T>(this IList<T> items, int partCount)
		{
			var partSize = items.Count / partCount;
			var extra = items.Count % partCount;
			var offset = 0;
			for (var i = 0; i < partCount; i++)
			{
				var currentPartSize = partSize;
				if (extra > 0)
				{
					currentPartSize++;
					extra--;
				}

				yield return items.Skip(offset).Take(currentPartSize).ToList();
				offset += currentPartSize;
			}
		}

		public static async Task<IReadOnlyList<R>> ParallelsAsync<S, R>(this IReadOnlyCollection<S> sourceList,
			Func<S, CancellationToken, Task<R>> actionTask, int maxParallelizm, CancellationToken token)
		{
			var collTasks = new List<Task<R>>(sourceList.Count);
			var throttler = new SemaphoreSlim(maxParallelizm);

			async Task<R> runAction(S source)
			{
				try
				{
					return await actionTask(source, token);
				}
				finally
				{
					throttler.Release();
				}
			}

			foreach (var source in sourceList)
			{
				await throttler.WaitAsync(token);
				collTasks.Add(runAction(source));
			}

			return await Task.WhenAll(collTasks);
		}

		public static async Task ParallelsAsync<S>(this IReadOnlyCollection<S> sourceList, Func<S, CancellationToken, Task> actionTask, int maxParallelizm, CancellationToken token)
		{
			var collTasks = new List<Task>(sourceList.Count);
			var throttler = new SemaphoreSlim(maxParallelizm);

			async Task runAction(S source)
			{
				try
				{
					await actionTask(source, token);
				}
				finally
				{
					throttler.Release();
				}
			}

			foreach (var source in sourceList)
			{
				await throttler.WaitAsync(token);
				collTasks.Add(runAction(source));
			}

			await Task.WhenAll(collTasks);
		}

		public static IReadOnlyList<TagRange> InRange(this IReadOnlyList<TagRange> tagRanges, BsonBound? intL, BsonBound? intR)
		{
			if(intL.HasValue && intR.HasValue)
				tagRanges = tagRanges.Where(r => crossInterval(intL.Value, intR.Value, r.Min, r.Max)).ToList();

			return tagRanges;
		}

		private static bool crossInterval(BsonBound intL, BsonBound intR, BsonBound chL, BsonBound chR)
		{
			return chL <= intR  && intL < chR;
		}
	}
}
