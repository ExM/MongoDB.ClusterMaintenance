namespace ShardEqualizer
{
	public interface IConfigDbRepositoryProvider
	{
		ChunkRepository Chunks { get; }
		CollectionRepository Collections { get; }
		TagRangeRepository Tags { get; }
		SettingsRepository Settings { get; }
	}
}
