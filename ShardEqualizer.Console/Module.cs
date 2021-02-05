using System;
using MongoDB.Driver;
using Ninject;
using Ninject.Modules;
using ShardEqualizer.LocalStoring;
using ShardEqualizer.Serialization;

namespace ShardEqualizer
{
	public class Module: NinjectModule
	{
		public override void Load()
		{
			CollectionNamespaceSerializer.Register();
			Bind<MongoClientBuilder>().ToSelf().InSingletonScope();
			Bind<ClusterIdService>().ToSelf().InSingletonScope();
			Bind<IAsyncDisposable, ProgressRenderer>().To<ProgressRenderer>().InSingletonScope();

			Bind<LocalStoreProvider>().ToSelf().InSingletonScope();

			Bind<IMongoClient>().ToMethod(ctx => ctx.Kernel.Get<MongoClientBuilder>().Build()).InSingletonScope();
			Bind<ConfigDBContainer>().ToMethod(ctx => new ConfigDBContainer(ctx.Kernel.Get<IMongoClient>())).InSingletonScope();

			Bind<ChunkRepository>().ToSelf().InSingletonScope()
				.WithConstructorArgument(ctx => Kernel.Get<ConfigDBContainer>().MongoDatabase);
			Bind<CollectionRepository>().ToSelf().InSingletonScope()
				.WithConstructorArgument(ctx => Kernel.Get<ConfigDBContainer>().MongoDatabase);
			Bind<TagRangeRepository>().ToSelf().InSingletonScope()
				.WithConstructorArgument(ctx => Kernel.Get<ConfigDBContainer>().MongoDatabase);
			Bind<SettingsRepository>().ToSelf().InSingletonScope()
				.WithConstructorArgument(ctx => Kernel.Get<ConfigDBContainer>().MongoDatabase);
			Bind<ShardRepository>().ToSelf().InSingletonScope()
				.WithConstructorArgument(ctx => Kernel.Get<ConfigDBContainer>().MongoDatabase);
			Bind<VersionRepository>().ToSelf().InSingletonScope()
				.WithConstructorArgument(ctx => Kernel.Get<ConfigDBContainer>().MongoDatabase);

			Bind<IAdminDB>().To<AdminDB>().InSingletonScope();

			Bind<ShardedCollectionService>().ToSelf().InSingletonScope();
			Bind<TagRangeService>().ToSelf().InSingletonScope();
			Bind<ClusterSettingsService>().ToSelf().InSingletonScope();
			Bind<ShardListService>().ToSelf().InSingletonScope();
			Bind<CollectionListService>().ToSelf().InSingletonScope();
			Bind<CollectionStatisticService>().ToSelf().InSingletonScope();
			Bind<ChunkService>().ToSelf().InSingletonScope();
			Bind<ChunkSizeService>().ToSelf().InSingletonScope();
		}
	}

	public class ConfigDBContainer
	{
		public ConfigDBContainer(IMongoClient mongoClient)
		{
			MongoDatabase = mongoClient.GetDatabase("config");
		}

		public IMongoDatabase MongoDatabase { get; }
	}
}
