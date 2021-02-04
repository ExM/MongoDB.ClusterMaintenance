using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Accord.Math.Optimization;
using MongoDB.Driver;
using ShardEqualizer.Models;

namespace ShardEqualizer.ShardSizeEqualizing
{
	public class ZoneOptimizationSolve
	{
		private ZoneOptimizationSolve(ZoneOptimizationDescriptor source)
		{
			var allBucketList = source.AllBuckets.Select(_ => new BucketSolve(_)).ToList();
			_bucketsByShardByCollection = allBucketList.GroupBy(_ => _.Shard)
				.ToDictionary(k => k.Key,
					v => (IReadOnlyDictionary<CollectionNamespace, BucketSolve>) v.ToDictionary(
						_ => _.Collection, _ => _));
			
			_managedBucketList = allBucketList.Where(b => source[b.Collection, b.Shard].Managed).ToList();
		}

		private void find(ZoneOptimizationDescriptor source, CancellationToken token)
		{
			var managedBucketsByCollection = _managedBucketList.GroupBy(_ => _.Collection)
				.ToDictionary(p => p.Key, p => p.ToList());

			var totalVariables = managedBucketsByCollection.Values
				.Select(_ => _.Count)
				.Where(c => c >= 2)
				.Sum(c => c - 1);

			var currentVar = 0;
			foreach (var buckets in managedBucketsByCollection.Values)
			{
				if (!buckets.Any())
					continue;
				var firstBucket = buckets.First();

				firstBucket.VariableFunction = new LinearPolinomial(totalVariables)
				{
					ConstantTerm = firstBucket.CurrentSize
				};

				foreach (var bucket in buckets.Skip(1))
				{
					firstBucket.VariableFunction.LinearTerms[currentVar] = -1;
					firstBucket.VariableFunction.ConstantTerm += bucket.CurrentSize;

					bucket.VariableIndex = currentVar;
					bucket.VariableFunction = new LinearPolinomial(totalVariables);
					bucket.VariableFunction.LinearTerms[currentVar] = 1;

					currentVar++;
				}
			}
		
			var avgManagedShardSize = (double) source.TotalManagedSize / source.Shards.Count;

			var targetShardSize = source.UnmovedSizeByShard.ToDictionary(_ => _.Key, _ => avgManagedShardSize - _.Value);

			var targetFunction = new QuadraticObjectiveFunction(
				new double[totalVariables, totalVariables],
				new double[totalVariables]);
			
			var managedBucketsByShard = _managedBucketList.GroupBy(_ => _.Shard)
				.ToDictionary(p => p.Key, p => p.ToList());
				
			foreach (var shard in managedBucketsByShard.Keys)
			{
				var sum = new LinearPolinomial(totalVariables)
				{
					ConstantTerm = -targetShardSize[shard]
				};

				foreach (var bucket in managedBucketsByShard[shard])
				{
					if(source.CollectionSettings[bucket.Collection].Adjustable)
						sum += bucket.VariableFunction;
				}

				targetFunction += sum.Square() * (source.ShardEqualsPriority / (avgManagedShardSize * avgManagedShardSize));
			}

			//Console.WriteLine(targetFunction.QuadraticTerms.ToCSharp());
			//Console.WriteLine(targetFunction.QuadraticTerms.Determinant());

			var avgCollectionSize = new Dictionary<CollectionNamespace, double>();

			foreach (var coll in managedBucketsByCollection.Keys)
			{
				var buckets = managedBucketsByCollection[coll];
				var sum = buckets.Sum(_ => _.CurrentSize);

				avgCollectionSize[coll] = (double) sum / buckets.Count;
			}

			foreach (var bucket in _managedBucketList)
			{
				var avg = avgCollectionSize[bucket.Collection];
				
				if(avg <= 0)
					continue;

				var sum = bucket.VariableFunction + new LinearPolinomial(totalVariables) {ConstantTerm = -avg};

				var error = sum.Square();

				targetFunction += error * (source.CollectionSettings[bucket.Collection].Priority / (avg * avg));
			}

			//Console.WriteLine(targetFunction.QuadraticTerms.ToCSharp());
			//Console.WriteLine(targetFunction.QuadraticTerms.Determinant());

			var constraints = new List<LinearConstraint>();

			var variablesAtIndices = Enumerable.Range(0, totalVariables).ToArray();

			var lowRate = 1 - source.DeviationLimitFromAverage;
			var highRate = 1 + source.DeviationLimitFromAverage;

			foreach (var bucket in _managedBucketList)
			{
				var avg = avgCollectionSize[bucket.Collection];

				var min = Math.Min(bucket.CurrentSize,
					Math.Max(avg * lowRate, source[bucket.Collection, bucket.Shard].MinSize));

				var max = Math.Max(bucket.CurrentSize, avg * highRate);

				if (max - min <= 1)
					max *= highRate;

				constraints.Add(new LinearConstraint(totalVariables)
				{
					VariablesAtIndices = variablesAtIndices,
					CombinedAs = bucket.VariableFunction.LinearTerms,
					ShouldBe = ConstraintType.GreaterThanOrEqualTo,
					Value = min - bucket.VariableFunction.ConstantTerm,
					Tolerance = 0.25
				});
				_constraintDescriptions.Add(new BucketConstraint(bucket, BucketConstraint.ConstraintType.Min, (long) Math.Round(min)));

				constraints.Add(new LinearConstraint(totalVariables)
				{
					VariablesAtIndices = variablesAtIndices,
					CombinedAs = bucket.VariableFunction.LinearTerms,
					ShouldBe = ConstraintType.LesserThanOrEqualTo,
					Value = max - bucket.VariableFunction.ConstantTerm,
					Tolerance = 0.25
				});
				_constraintDescriptions.Add(new BucketConstraint(bucket, BucketConstraint.ConstraintType.Max, (long) Math.Round(max)));
			}
			
			var initialVector = new double[totalVariables];

			foreach (var bucket in _managedBucketList.Where(_ => _.VariableIndex.HasValue))
			{
				initialVector[bucket.VariableIndex.Value] = bucket.CurrentSize;
			}

			var solver = new GoldfarbIdnani(targetFunction, constraints) {Token = token};

			IsSuccess = solver.Minimize(initialVector);
			if (!IsSuccess)
				return;
			
			foreach (var bucket in _managedBucketList)
				bucket.TargetSize = (long) Math.Round(bucket.VariableFunction.Function(solver.Solution));

			TargetShards = _bucketsByShardByCollection.ToDictionary(
				_ => _.Key,
				_ => _.Value.Values.Select(b => b.TargetSize).Sum() + source.UnShardedSize[_.Key]);
			
			TargetShardMaxDeviation = TargetShards.Values.Max() - TargetShards.Values.Min();
		}

		public static ZoneOptimizationSolve Find(ZoneOptimizationDescriptor source) => Find(source, CancellationToken.None);
		
		public static ZoneOptimizationSolve Find(ZoneOptimizationDescriptor source, CancellationToken token)
		{
			var solve = new ZoneOptimizationSolve(source);
			
			solve.find(source, token);
			
			return solve;
		}

		public BucketSolve this[CollectionNamespace coll, ShardIdentity shard] =>
			_bucketsByShardByCollection[shard][coll];
		
		public IReadOnlyDictionary<ShardIdentity, long> TargetShards { get; private set; }

		public long TargetShardMaxDeviation { get; private set; }
		
		public List<BucketConstraint> ActiveConstraints => _constraintDescriptions.Where(_ => _.IsActive).ToList();
		
		public IReadOnlyList<BucketSolve> AllManagedBuckets => _managedBucketList;
		
		private readonly IReadOnlyDictionary<ShardIdentity, IReadOnlyDictionary<CollectionNamespace, BucketSolve>> _bucketsByShardByCollection;
		private readonly IReadOnlyList<BucketSolve> _managedBucketList;
		
		private readonly List<BucketConstraint> _constraintDescriptions = new List<BucketConstraint>();
		public bool IsSuccess { get; private set; }
	}
}