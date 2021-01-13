using MongoDB.Driver;

namespace ShardEqualizer
{
	public class ConfigDbRepositoryProvider : IConfigDbRepositoryProvider
	{
		public ConfigDbRepositoryProvider(IMongoClient client)
		{
			var db = client.GetDatabase("config");

			Chunks = new ChunkRepository(db);
			Collections = new CollectionRepository(db);
			Tags = new TagRangeRepository(db);
			Shards = new ShardRepository(db);
			Settings = new SettingsRepository(db);
			Version = new VersionRepository(db);
		}

		public ChunkRepository Chunks { get; }

		public CollectionRepository Collections { get; }

		public TagRangeRepository Tags { get; }

		public ShardRepository Shards { get; }

		public SettingsRepository Settings { get; }

		public VersionRepository Version { get; }

	}
}
