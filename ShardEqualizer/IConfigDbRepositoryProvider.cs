namespace ShardEqualizer
{
	public interface IConfigDbRepositoryProvider
	{
		ChunkRepository Chunks { get; }
		TagRangeRepository Tags { get; }
		SettingsRepository Settings { get; }
	}
}
