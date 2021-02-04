using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ShardEqualizer.Models;

namespace ShardEqualizer.ShardSizeEqualizing
{
	public class ZoneOptimizationDescriptor: IUnShardedSizeDescriptor, ICollectionSettingsDescriptor
	{
		public ZoneOptimizationDescriptor(IEnumerable<CollectionNamespace> collections, IEnumerable<ShardIdentity> shards)
		{
			Collections = collections.ToList();
			foreach (var coll in Collections)
				_collectionSettings.Add(coll, new CollectionSettings(){ Priority = 1, Adjustable = true });

			Shards = shards.ToList();
			foreach (var shard in Shards)
				_unShardedSizes.Add(shard, 0);

			_bucketList = Collections.SelectMany(coll => Shards.Select(sh => new Bucket(coll, sh))).ToList();

			_bucketsByShardByCollection =  _bucketList.GroupBy(_ => _.Shard)
				.ToDictionary(k => k.Key, v => (IReadOnlyDictionary<CollectionNamespace, Bucket>)v.ToDictionary(_ => _.Collection, _ => _));

			_bucketsByShard = _bucketList.GroupBy(_ => _.Shard)
				.ToDictionary(_ => _.Key, _ => (IReadOnlyList<Bucket>) _.ToList());

			ShardEqualsPriority = 100;
			DeviationLimitFromAverage = 0.5;
		}

		public Bucket this[CollectionNamespace coll, ShardIdentity shard] =>
			_bucketsByShardByCollection[shard][coll];

		long IUnShardedSizeDescriptor.this[ShardIdentity shard]
		{
			get => _unShardedSizes[shard];
			set =>  _unShardedSizes[shard] = value;
		}

		CollectionSettings ICollectionSettingsDescriptor.this[CollectionNamespace coll] => _collectionSettings[coll];

		public double ShardEqualsPriority { get; set; }

		/// <summary>
		/// limit of deviation from the average value of the bucket in percent
		/// </summary>
		public double DeviationLimitFromAverage { get; set; }

		public IUnShardedSizeDescriptor UnShardedSize => this;

		public ICollectionSettingsDescriptor CollectionSettings => this;

		public IReadOnlyList<Bucket> AllManagedBuckets => _bucketList.Where(_ => _.Managed).ToList();

		public IReadOnlyList<Bucket> AllBuckets => _bucketList;

		public IReadOnlyDictionary<ShardIdentity, long> UnmovedSizeByShard =>
			_bucketsByShard.ToDictionary(
				p => p.Key,
				p => _unShardedSizes[p.Key] + p.Value.Where(b => !b.Managed).Sum(b => b.CurrentSize));

		public long TotalManagedSize => AllManagedBuckets.Select(_ => _.CurrentSize).Sum();

		public IReadOnlyList<CollectionNamespace> Collections { get; }
		public IReadOnlyList<ShardIdentity> Shards { get; }

		private readonly IDictionary<ShardIdentity, long> _unShardedSizes = new Dictionary<ShardIdentity, long>();

		private readonly IDictionary<CollectionNamespace, CollectionSettings> _collectionSettings = new Dictionary<CollectionNamespace, CollectionSettings>();

		private readonly IReadOnlyDictionary<ShardIdentity, IReadOnlyList<Bucket>> _bucketsByShard;
		private readonly IReadOnlyDictionary<ShardIdentity, IReadOnlyDictionary<CollectionNamespace, Bucket>> _bucketsByShardByCollection;
		private readonly IReadOnlyList<Bucket> _bucketList;

		public static ZoneOptimizationDescriptor Deserialize(string text)
		{
			var container = JsonConvert.DeserializeObject<Container>(text);

			var result = new ZoneOptimizationDescriptor(
				container.Collections.Select(_ => CollectionNamespace.FromFullName(_.Ns)),
				container.Shards.Select(_ => new ShardIdentity(_.Name)));

			result.ShardEqualsPriority = container.ShardEqualsPriority;
			result.DeviationLimitFromAverage = container.DeviationLimitFromAverage;

			foreach (var collDesc in container.Collections)
			{
				var collSettings = result.CollectionSettings[CollectionNamespace.FromFullName(collDesc.Ns)];
				collSettings.Priority = collDesc.Priority;
				collSettings.Adjustable = collDesc.Adjustable;
			}

			foreach (var shardDesc in container.Shards)
			{
				var shardId = new ShardIdentity(shardDesc.Name);
				result.UnShardedSize[shardId] = shardDesc.UnShardedSize;
			}

			foreach (var bucketDesc in container.Buckets)
			{
				var collNs = CollectionNamespace.FromFullName(bucketDesc.Collection);
				var shardId = new ShardIdentity(bucketDesc.Shard);
				var bucket = result[collNs, shardId];

				bucket.Managed = bucketDesc.Managed;
				bucket.CurrentSize = bucketDesc.Size;
				bucket.MinSize = bucketDesc.Min;
			}

			return result;
		}

		public string Serialize()
		{
			var container = new Container()
			{
				ShardEqualsPriority = ShardEqualsPriority,
				DeviationLimitFromAverage = DeviationLimitFromAverage,
				Collections = Collections.Select(_ => new CollectionDescriptor()
				{
					Ns = _.FullName,
					Priority = CollectionSettings[_].Priority,
					Adjustable = CollectionSettings[_].Adjustable

				}).ToArray(),
				Shards = Shards.Select(_ => new ShardDescriptor()
				{
					Name = _.ToString(),
					UnShardedSize = UnShardedSize[_]
				}).ToArray(),
				Buckets = AllBuckets.Select(_ => new BucketDescriptor()
				{
					Collection = _.Collection.FullName,
					Shard = _.Shard.ToString(),
					Size = _.CurrentSize,
					Managed = _.Managed,
					Min = _.MinSize
				}).ToArray()
			};

			return JObject.FromObject(container).ToString();
		}

		private class Container
		{
			public double ShardEqualsPriority;
			public double DeviationLimitFromAverage;
			public CollectionDescriptor[] Collections;
			public ShardDescriptor[] Shards;
			public BucketDescriptor[] Buckets;
		}

		private class BucketDescriptor
		{
			public string Collection;
			public string Shard;
			public long Size;
			public bool Managed;
			public long Min;
		}

		private class CollectionDescriptor
		{
			public string Ns;
			public double Priority;
			public bool Adjustable;
		}

		private class ShardDescriptor
		{
			public string Name;
			public long UnShardedSize;
		}
	}

	public interface ICollectionSettingsDescriptor
	{
		CollectionSettings this[CollectionNamespace coll] { get; }
	}

	public interface IUnShardedSizeDescriptor
	{
		long this[ShardIdentity shard] { get; set; }
	}

	public class CollectionSettings
	{
		public double Priority { get; set; }
		public bool Adjustable { get; set; }
	}
}
