using MongoDB.Driver;

namespace ShardEqualizer
{
	public class ConfigDbRepositoryProvider : IConfigDbRepositoryProvider
	{
		public ConfigDbRepositoryProvider(IMongoClient client)
		{
			var db = client.GetDatabase("config");

			Chunks = new ChunkRepository(db);
			Tags = new TagRangeRepository(db);
			Settings = new SettingsRepository(db);
		}

		public ChunkRepository Chunks { get; }

		public TagRangeRepository Tags { get; }

		public SettingsRepository Settings { get; }
	}
}
