using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;
using ShardEqualizer.LocalStoring;

namespace ShardEqualizer
{
	public class ClusterSettingsService
	{
		private readonly SettingsRepository _repo;
		private readonly ProgressRenderer _progressRenderer;
		private readonly LocalStore<SettingsContainer> _store;

		public ClusterSettingsService(
			SettingsRepository repo,
			ProgressRenderer progressRenderer,
			LocalStoreProvider storeProvider)
		{
			_repo = repo;
			_progressRenderer = progressRenderer;

			_store = storeProvider.Create<SettingsContainer>("settings", onSave);
		}

		private void onSave(SettingsContainer container)
		{
		}

		public async Task<long> GetChunkSize(CancellationToken token)
		{
			if (_store.Container.ChunkSize == null)
			{
				await using var reporter = _progressRenderer.Start("Load settings");
				_store.Container.ChunkSize = await _repo.GetChunksize(token);
				_store.OnChanged();
			}

			return _store.Container.ChunkSize.Value;
		}

		private class SettingsContainer: Container
		{
			[BsonElement("chunkSize"), BsonRequired]
			public long? ChunkSize { get; set; }
		}
	}
}
