using System;
using System.Collections.Generic;
using System.Linq;
using Accord.Math.Optimization;
using MongoDB.ClusterMaintenance.Models;
using MongoDB.ClusterMaintenance.ShardSizeEqualizing;
using MongoDB.Driver;
using NUnit.Framework;

namespace MongoDB.ClusterMaintenance
{
	[TestFixture]
	public class ZoneOptimizationByGoldfarbIdnaniTests
	{
		[Test]
		public void SizeSolverDemo()
		{
			var zoneOpt = new ZoneOptimizationDescriptor(
				new []{_cA, _cB, _cC, _cD, _cE},
				new []{_sA, _sB, _sC, _sD});
			
			zoneOpt.UnShardedSize[_sA] = 100;
			zoneOpt.UnShardedSize[_sB] = 20;
			zoneOpt.UnShardedSize[_sD] = 30;
			
			zoneOpt[_cA, _sA].Init(b => { b.CurrentSize =  600; b.Managed = false;});
			zoneOpt[_cA, _sB].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			zoneOpt[_cA, _sC].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			zoneOpt[_cA, _sD].Init(b => { b.CurrentSize = 1230; b.Managed = true;});
			
			zoneOpt[_cB, _sA].Init(b => { b.CurrentSize =  100; b.Managed = true;});
			zoneOpt[_cB, _sB].Init(b => { b.CurrentSize = 4520; b.Managed = true;});
			zoneOpt[_cB, _sC].Init(b => { b.CurrentSize =   30; b.Managed = true;});
			zoneOpt[_cB, _sD].Init(b => { b.CurrentSize = 2330; b.Managed = true;});

			zoneOpt[_cC, _sA].Init(b => { b.CurrentSize =  100; b.Managed = true;});
			zoneOpt[_cC, _sB].Init(b => { b.CurrentSize =  100; b.Managed = true;});
			zoneOpt[_cC, _sC].Init(b => { b.CurrentSize =    0; b.Managed = true;});
			zoneOpt[_cC, _sD].Init(b => { b.CurrentSize =   30; b.Managed = true;});
			
			zoneOpt[_cD, _sA].Init(b => { b.CurrentSize =  100; b.Managed = true;});
			zoneOpt[_cD, _sB].Init(b => { b.CurrentSize =  200; b.Managed = true;});
			zoneOpt[_cD, _sC].Init(b => { b.CurrentSize =  500; b.Managed = true;});
			zoneOpt[_cD, _sD].Init(b => { b.CurrentSize =  300; b.Managed = true;});

			zoneOpt[_cE, _sA].Init(b => { b.CurrentSize =   10; b.Managed = true;});
			zoneOpt[_cE, _sB].Init(b => { b.CurrentSize =   20; b.Managed = true;});
			zoneOpt[_cE, _sC].Init(b => { b.CurrentSize =   20; b.Managed = true;});
			zoneOpt[_cE, _sD].Init(b => { b.CurrentSize =   30; b.Managed = false;});

			var solve = ZoneOptimizationSolve.Find(zoneOpt);

			Assert.IsTrue(solve.IsSuccess);
		}
		
		[Test]
		public void BlockSizeReduction()
		{
			var zoneOpt = new ZoneOptimizationDescriptor(
				new []{_cA, _cB, _cC},
				new []{_sA, _sB, _sC});
			
			zoneOpt[_cA, _sA].Init(b => { b.CurrentSize =    0; b.Managed = false;});
			zoneOpt[_cA, _sB].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			zoneOpt[_cA, _sC].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			
			zoneOpt[_cB, _sA].Init(b => { b.CurrentSize = 3000; b.Managed = true; });
			zoneOpt[_cB, _sB].Init(b => { b.CurrentSize = 4000; b.Managed = true; b.BlockSizeReduction(); });
			zoneOpt[_cB, _sC].Init(b => { b.CurrentSize = 2000; b.Managed = true; });
			
			zoneOpt[_cC, _sA].Init(b => { b.CurrentSize =  500; b.Managed = true; });
			zoneOpt[_cC, _sB].Init(b => { b.CurrentSize =  500; b.Managed = true; });
			zoneOpt[_cC, _sC].Init(b => { b.CurrentSize =  500; b.Managed = true; });

			zoneOpt.ShardEqualsPriority = 100;
			
			var solve = ZoneOptimizationSolve.Find(zoneOpt);

			Assert.IsTrue(solve.IsSuccess);

			Assert.That(solve[_cB, _sB].TargetSize, Is.EqualTo(4000));
			Assert.That(solve[_cC, _sA].TargetSize, Is.EqualTo(750));
			Assert.That(solve[_cC, _sB].TargetSize, Is.EqualTo(250));
			Assert.That(solve[_cC, _sC].TargetSize, Is.EqualTo(500));

			Assert.That(solve.ActiveConstraints.Count, Is.EqualTo(6));
		}
		
		[Test]
		public void WithoutUnShardCompensation()
		{
			var zoneOpt = new ZoneOptimizationDescriptor(
				new []{_cA, _cB, _cC},
				new []{_sA, _sB, _sC});
			
			zoneOpt[_cA, _sA].Init(b => { b.CurrentSize =    0; b.Managed = false;});
			zoneOpt[_cA, _sB].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			zoneOpt[_cA, _sC].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			
			zoneOpt[_cB, _sA].Init(b => { b.CurrentSize = 3000; b.Managed = true; });
			zoneOpt[_cB, _sB].Init(b => { b.CurrentSize = 4000; b.Managed = true; });
			zoneOpt[_cB, _sC].Init(b => { b.CurrentSize = 2000; b.Managed = true; });
			
			zoneOpt[_cC, _sA].Init(b => { b.CurrentSize =  600; b.Managed = true; });
			zoneOpt[_cC, _sB].Init(b => { b.CurrentSize =  500; b.Managed = true; });
			zoneOpt[_cC, _sC].Init(b => { b.CurrentSize =  400; b.Managed = true; });

			zoneOpt.ShardEqualsPriority = 100;
			zoneOpt.CollectionSettings[_cC].UnShardCompensation = false;
			
			var solve = ZoneOptimizationSolve.Find(zoneOpt);

			Assert.IsTrue(solve.IsSuccess);

			Assert.That(solve[_cB, _sA].TargetSize, Is.EqualTo(4300));
			Assert.That(solve[_cB, _sB].TargetSize, Is.EqualTo(2350));
			Assert.That(solve[_cC, _sA].TargetSize, Is.EqualTo(500));
			Assert.That(solve[_cC, _sB].TargetSize, Is.EqualTo(500));
			Assert.That(solve[_cC, _sC].TargetSize, Is.EqualTo(500));

			Assert.That(solve.ActiveConstraints, Is.Empty);
		}
		
		[Test]
		public void NoManagedCollection()
		{
			var zoneOpt = new ZoneOptimizationDescriptor(
				new []{_cA, _cB, _cC},
				new []{_sA, _sB, _sC});
			
			zoneOpt[_cA, _sA].Init(b => { b.CurrentSize =    0; b.Managed = false;});
			zoneOpt[_cA, _sB].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			zoneOpt[_cA, _sC].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			
			zoneOpt[_cB, _sA].Init(b => { b.CurrentSize = 3000; b.Managed = true; });
			zoneOpt[_cB, _sB].Init(b => { b.CurrentSize = 3000; b.Managed = true; });
			zoneOpt[_cB, _sC].Init(b => { b.CurrentSize = 3000; b.Managed = true; });
			
			zoneOpt[_cC, _sA].Init(b => { b.CurrentSize =  500; b.Managed = false; });
			zoneOpt[_cC, _sB].Init(b => { b.CurrentSize =  500; b.Managed = false; });
			zoneOpt[_cC, _sC].Init(b => { b.CurrentSize =  500; b.Managed = false; });

			zoneOpt.ShardEqualsPriority = 1;
			
			var solve = ZoneOptimizationSolve.Find(zoneOpt);

			Assert.IsTrue(solve.IsSuccess);

			Assert.That(solve[_cB, _sB].TargetSize, Is.EqualTo(2784));
			Assert.That(solve[_cC, _sA].TargetSize, Is.EqualTo(500));
			Assert.That(solve[_cC, _sB].TargetSize, Is.EqualTo(500));
			Assert.That(solve[_cC, _sC].TargetSize, Is.EqualTo(500));
		}
		
		[Test]
		public void ExcessConstraints()
		{
			var zoneOpt = new ZoneOptimizationDescriptor(
				new []{_cA, _cB, _cC},
				new []{_sA, _sB, _sC});
			
			zoneOpt[_cA, _sA].Init(b => { b.CurrentSize =    0; b.Managed = false;});
			zoneOpt[_cA, _sB].Init(b => { b.CurrentSize = 1000; b.Managed = true;});
			zoneOpt[_cA, _sC].Init(b => { b.CurrentSize = 3000; b.Managed = true;});
			
			zoneOpt[_cB, _sA].Init(b => { b.CurrentSize = 3000; b.Managed = true; b.BlockSizeReduction(); });
			zoneOpt[_cB, _sB].Init(b => { b.CurrentSize = 3000; b.Managed = true; b.BlockSizeReduction(); });
			zoneOpt[_cB, _sC].Init(b => { b.CurrentSize = 1000; b.Managed = true; b.BlockSizeReduction(); });
			
			zoneOpt[_cC, _sA].Init(b => { b.CurrentSize = 1000; b.Managed = true; });
			zoneOpt[_cC, _sB].Init(b => { b.CurrentSize =  100; b.Managed = true; });
			zoneOpt[_cC, _sC].Init(b => { b.CurrentSize =  100; b.Managed = true; });

			zoneOpt.ShardEqualsPriority = 1;
			
			var solve = ZoneOptimizationSolve.Find(zoneOpt);

			Assert.IsTrue(solve.IsSuccess);
			
			Assert.That(solve.ActiveConstraints.Count, Is.EqualTo(3));

			var activeConstraint = solve.ActiveConstraints.Single(_ => _.Bucket.Collection == _cB && _.Bucket.Shard == _sA);
			
			Assert.That(activeConstraint.Bound, Is.EqualTo(3000));
			Assert.That(activeConstraint.Bucket.Collection, Is.EqualTo(_cB));
			Assert.That(activeConstraint.Bucket.Shard, Is.EqualTo(_sA));
			Assert.That(activeConstraint.Type, Is.EqualTo(BucketConstraint.ConstraintType.Min));
			
			Assert.That(solve[_cA, _sB].TargetSize, Is.EqualTo(1807));
			Assert.That(solve[_cB, _sB].TargetSize, Is.EqualTo(3000));
			Assert.That(solve[_cC, _sA].TargetSize, Is.EqualTo(406));
			Assert.That(solve[_cC, _sB].TargetSize, Is.EqualTo(389));
		}

		[Test]
		public void SolveStability(
			[Values(100, 1000, 2000, 3500)] int bucketAB,
			[Values(10, 1000, 2000, 3000, 5500)] int bucketBA,
			[Values(10, 1000, 2000, 3000, 3200)] int bucketBB,
			[Values(10, 50, 125, 432, 499)] int bucketCA)
		{
			var zoneOpt = new ZoneOptimizationDescriptor(
				new []{_cA, _cB, _cC},
				new []{_sA, _sB, _sC});
			
			zoneOpt[_cA, _sA].Init(b => { b.CurrentSize =    0; b.Managed = false;});
			zoneOpt[_cA, _sB].Init(b => { b.CurrentSize = bucketAB; b.Managed = true;});
			zoneOpt[_cA, _sC].Init(b => { b.CurrentSize = 4000 - bucketAB; b.Managed = true;});
			
			zoneOpt[_cB, _sA].Init(b => { b.CurrentSize = bucketBA; b.Managed = true; });
			zoneOpt[_cB, _sB].Init(b => { b.CurrentSize = bucketBB; b.Managed = true; });
			zoneOpt[_cB, _sC].Init(b => { b.CurrentSize = 9000 - bucketBA - bucketBB; b.Managed = true; });
			
			zoneOpt[_cC, _sA].Init(b => { b.CurrentSize =  bucketCA; b.Managed = true; });
			zoneOpt[_cC, _sB].Init(b => { b.CurrentSize =  500; b.Managed = true; });
			zoneOpt[_cC, _sC].Init(b => { b.CurrentSize =  1000 - bucketCA; b.Managed = true; });

			zoneOpt.ShardEqualsPriority = 10;
			
			var solve = ZoneOptimizationSolve.Find(zoneOpt);

			Assert.IsTrue(solve.IsSuccess);
			
			var targetShards = solve.TargetShards;
			
			Assert.That(solve[_cA, _sB].TargetSize, Is.EqualTo(2000));
			
			Assert.That(solve[_cB, _sA].TargetSize, Is.EqualTo(4036));
			Assert.That(solve[_cB, _sB].TargetSize, Is.EqualTo(2482));
			
			Assert.That(solve[_cC, _sA].TargetSize, Is.EqualTo(529));

			Assert.That(targetShards.Values, Is.EquivalentTo(new []{4565, 4968, 4968}));
		}
		
		[Test]
		public void SolveStabilityWithBlockedSizeReduction(
			[Values(100, 1000, 2000, 3500)] int bucketAB,
			[Values(10, 1000, 2000, 3000, 5500)] int bucketBA,
			[Values(10, 50, 125, 432, 499)] int bucketCA,
			[Values(10, 50, 125, 432, 499)] int bucketCB,
			[Values(0.1, 0.25, 0.5, 0.75, 0.99)] double pathPercent)
		{
			var zoneOpt = new ZoneOptimizationDescriptor(
				new []{_cA, _cB, _cC},
				new []{_sA, _sB, _sC});
			
			zoneOpt[_cA, _sA].Init(b => { b.CurrentSize =    0; b.Managed = false;});
			zoneOpt[_cA, _sB].Init(b => { b.CurrentSize = bucketAB; b.Managed = true;});
			zoneOpt[_cA, _sC].Init(b => { b.CurrentSize = 4000 - bucketAB; b.Managed = true;});
			
			zoneOpt[_cB, _sA].Init(b => { b.CurrentSize = bucketBA; b.Managed = true; });
			zoneOpt[_cB, _sB].Init(b => { b.CurrentSize = 4500; b.Managed = true; b.BlockSizeReduction(); });
			zoneOpt[_cB, _sC].Init(b => { b.CurrentSize = 4500 - bucketBA; b.Managed = true; });
			
			zoneOpt[_cC, _sA].Init(b => { b.CurrentSize =  bucketCA; b.Managed = true; });
			zoneOpt[_cC, _sB].Init(b => { b.CurrentSize =  bucketCB; b.Managed = true; });
			zoneOpt[_cC, _sC].Init(b => { b.CurrentSize =  1500 - bucketCA - bucketCB; b.Managed = true; });

			zoneOpt.ShardEqualsPriority = 10;
			
			var solve = ZoneOptimizationSolve.Find(zoneOpt);

			Assert.IsTrue(solve.IsSuccess);
			var expectedTargets = solve.AllManagedBuckets.Select(_ => _.TargetSize).ToList();

			foreach (var b in zoneOpt.AllManagedBuckets)
			{
				b.CurrentSize = b.CurrentSize + (long)Math.Round(pathPercent * (solve[b.Collection, b.Shard].TargetSize - b.CurrentSize));
			}
			
			var solve2 = ZoneOptimizationSolve.Find(zoneOpt);
			
			Assert.IsTrue(solve2.IsSuccess);

			var actualTargets = solve2.AllManagedBuckets.Select(_ => _.TargetSize);

			foreach (var pair in actualTargets.Zip(expectedTargets, (actual, expected) => new {actual, expected}))
			{
				Assert.That(pair.actual, Is.EqualTo(pair.expected).Within(1));
			}
		}
		
		[TestCase(1,    new [] {3878, 5311, 5311}, 510)]
		[TestCase(2,    new [] {4089, 5205, 5205}, 516)]
		[TestCase(5,    new [] {4386, 5057, 5057}, 524)]
		[TestCase(10,   new [] {4565, 4968, 4968}, 529)]
		[TestCase(50,   new [] {4769, 4866, 4866}, 534)]
		[TestCase(100,  new [] {4800, 4849, 4849}, 535)]
		[TestCase(500,  new [] {4827, 4837, 4837}, 536)]
		[TestCase(1000, new [] {4830, 4835, 4835}, 536)]
		public void ShardEqualsPriority(double shardEqualsPriority, int[] expectedShards, long bucketCA)
		{
			var zoneOpt = new ZoneOptimizationDescriptor(
				new []{_cA, _cB, _cC},
				new []{_sA, _sB, _sC});
			
			zoneOpt[_cA, _sA].Init(b => { b.CurrentSize =    0; b.Managed = false;});
			zoneOpt[_cA, _sB].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			zoneOpt[_cA, _sC].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			
			zoneOpt[_cB, _sA].Init(b => { b.CurrentSize = 4000; b.Managed = true; });
			zoneOpt[_cB, _sB].Init(b => { b.CurrentSize = 3000; b.Managed = true; });
			zoneOpt[_cB, _sC].Init(b => { b.CurrentSize = 2000; b.Managed = true; });
			
			zoneOpt[_cC, _sA].Init(b => { b.CurrentSize =  500; b.Managed = true; });
			zoneOpt[_cC, _sB].Init(b => { b.CurrentSize =  500; b.Managed = true; });
			zoneOpt[_cC, _sC].Init(b => { b.CurrentSize =  500; b.Managed = true; });

			zoneOpt.ShardEqualsPriority = shardEqualsPriority;
			
			var solve = ZoneOptimizationSolve.Find(zoneOpt);

			Assert.IsTrue(solve.IsSuccess);
			
			var targetShards = solve.TargetShards;
			
			Assert.That(solve[_cC, _sA].TargetSize, Is.EqualTo(bucketCA));
			Assert.That(targetShards.Values, Is.EquivalentTo(expectedShards));
		}
		
		[TestCase(1,    new [] {4607, 2696, 2696})]
		[TestCase(50,   new [] {3825, 3087, 3087})]
		[TestCase(100,  new [] {3652, 3174, 3174})]
		[TestCase(500,  new [] {3417, 3292, 3292})]
		[TestCase(1000, new [] {3377, 3312, 3312})]
		[TestCase(5000, new [] {3342, 3329, 3329})]
		public void CollectionEqualsPriority(double collectionEqualsPriority, int[] expectedShards)
		{
			var zoneOpt = new ZoneOptimizationDescriptor(
				new []{_cA, _cB, _cC},
				new []{_sA, _sB, _sC});
			
			zoneOpt[_cA, _sA].Init(b => { b.CurrentSize =    0; b.Managed = false;});
			zoneOpt[_cA, _sB].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			zoneOpt[_cA, _sC].Init(b => { b.CurrentSize = 2000; b.Managed = true;});
			
			zoneOpt[_cB, _sA].Init(b => { b.CurrentSize = 5000; b.Managed = true; });
			zoneOpt[_cB, _sB].Init(b => { b.CurrentSize = 4000; b.Managed = true; });
			zoneOpt[_cB, _sC].Init(b => { b.CurrentSize = 1000; b.Managed = true; });
			
			zoneOpt[_cC, _sA].Init(b => { b.CurrentSize =  500; b.Managed = true; });
			zoneOpt[_cC, _sB].Init(b => { b.CurrentSize =  500; b.Managed = true; });
			zoneOpt[_cC, _sC].Init(b => { b.CurrentSize =  500; b.Managed = true; });

			zoneOpt.CollectionSettings[ _cB].Priority = collectionEqualsPriority;
			
			var solve = ZoneOptimizationSolve.Find(zoneOpt);

			Assert.IsTrue(solve.IsSuccess);
			
			Assert.That(new []
			{
				solve[_cB, _sA].TargetSize,
				solve[_cB, _sB].TargetSize,
				solve[_cB, _sC].TargetSize
			}, Is.EquivalentTo(expectedShards));
		}
		
		[Test]
		public void Trivial()
		{
			var zoneOpt = new ZoneOptimizationDescriptor(
				new []{_cA, _cB},
				new []{_sA, _sB});

			zoneOpt.UnShardedSize[_sA] = 0;
			zoneOpt.UnShardedSize[_sB] = 0;
			
			zoneOpt[_cA, _sA].Init(b =>
			{
				 b.CurrentSize = 1000;
				 b.Managed = true;
			});
			zoneOpt[_cA, _sB].Init(b =>
			{
				b.CurrentSize = 3000;
				b.Managed = true;
			});
			zoneOpt[_cB, _sA].Init(b =>
			{
				b.CurrentSize = 3000;
				b.Managed = true;
			});
			zoneOpt[_cB, _sB].Init(b =>
			{
				b.CurrentSize = 1000;
				b.Managed = true;
			});

			var solve = ZoneOptimizationSolve.Find(zoneOpt);

			Assert.IsTrue(solve.IsSuccess);
			
			Assert.That(solve[_cA, _sA].TargetSize, Is.EqualTo(2000));
			Assert.That(solve[_cA, _sB].TargetSize, Is.EqualTo(2000));
			Assert.That(solve[_cB, _sA].TargetSize, Is.EqualTo(2000));
			Assert.That(solve[_cB, _sB].TargetSize, Is.EqualTo(2000));
		}
		
		private static readonly ShardIdentity _sA = new ShardIdentity("shA");
		private static readonly ShardIdentity _sB = new ShardIdentity("shB");
		private static readonly ShardIdentity _sC = new ShardIdentity("shC");
		private static readonly ShardIdentity _sD = new ShardIdentity("shD");
			
		private static readonly CollectionNamespace _cA = new CollectionNamespace("d", "collA");
		private static readonly CollectionNamespace _cB = new CollectionNamespace("d", "collB");
		private static readonly CollectionNamespace _cC = new CollectionNamespace("d", "collC");
		private static readonly CollectionNamespace _cD = new CollectionNamespace("d", "collD");
		private static readonly CollectionNamespace _cE = new CollectionNamespace("d", "collE");
	}
}