using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using ShardEqualizer.LocalStoring;

namespace ShardEqualizer
{
	public class UserCollectionsLocalStore
	{
		private UserCollections _userCollections;

		private readonly LocalStore<UserCollectionsContainer> _store;

		public UserCollectionsLocalStore(LocalStoreProvider storeProvider)
		{
			_store = storeProvider.Create<UserCollectionsContainer>("userCollections", onSave);

			if (_store.Container.AllUserCollections != null)
				_userCollections = new UserCollections(_store.Container.AllUserCollections);
		}

		private void onSave(UserCollectionsContainer container)
		{
			container.AllUserCollections = _userCollections;
		}

		public UserCollections TryGet()
		{
			return _userCollections;
		}

		public void Save(UserCollections userCollections)
		{
			_userCollections = userCollections;
			_store.OnChanged();
		}

		private class UserCollectionsContainer: Container
		{
			[BsonElement("allUserCollections"), BsonRequired]
			public IList<CollectionNamespace> AllUserCollections { get; set; }
		}
	}
}
