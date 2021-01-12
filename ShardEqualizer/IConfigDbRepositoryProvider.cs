namespace MongoDB.ClusterMaintenance
{
	public interface IConfigDbRepositoryProvider
	{
		ChunkRepository Chunks { get; }
		CollectionRepository Collections { get; }
		TagRangeRepository Tags { get; }
		ShardRepository Shards { get; }
		SettingsRepository Settings { get; }
	}
}