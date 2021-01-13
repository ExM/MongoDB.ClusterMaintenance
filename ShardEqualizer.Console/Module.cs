using MongoDB.Driver;
using Ninject;
using Ninject.Modules;
using ShardEqualizer.Serialization;

namespace ShardEqualizer
{
	public class Module: NinjectModule
	{
		public override void Load()
		{
			CollectionNamespaceSerializer.Register();
			Bind<MongoClientBuilder>().ToSelf().InSingletonScope();
			Bind<ClusterIdValidator>().ToSelf().InSingletonScope();
			Bind<IMongoClient>().ToMethod(ctx => ctx.Kernel.Get<MongoClientBuilder>().Build()).InSingletonScope();
			Bind<IConfigDbRepositoryProvider>().To<ConfigDbRepositoryProvider>().InSingletonScope();
			Bind<IAdminDB>().To<AdminDB>().InSingletonScope();
		}
	}
}
