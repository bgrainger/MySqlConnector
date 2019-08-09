using System;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query.ExpressionTranslators;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection.Extensions;
using MySqlConnector.EntityFrameworkCore.MySql.Query.ExpressionTranslators.Internal;
using MySqlConnector.EntityFrameworkCore.MySql.Storage.Internal;
using NetTopologySuite;

namespace Microsoft.Extensions.DependencyInjection
{
	/// <summary>
	/// EntityFrameworkCore.Sqlite.NetTopologySuite extension methods for <see cref="IServiceCollection" />.
	/// </summary>
	public static class MySqlConnectorNetTopologySuiteServiceCollectionExtensions
	{
		/// <summary>
		/// Adds the services required for NetTopologySuite support in the MySQL provider for Entity Framework.
		/// </summary>
		/// <param name="serviceCollection">The <see cref="IServiceCollection" /> to add services to.</param>
		/// <returns>The same service collection so that multiple calls can be chained.</returns>
		public static IServiceCollection AddEntityFrameworkMySqlConnectorNetTopologySuite(this IServiceCollection serviceCollection)
		{
			if (serviceCollection is null)
				throw new ArgumentNullException(nameof(serviceCollection));

			serviceCollection.TryAddSingleton(NtsGeometryServices.Instance);

			new EntityFrameworkRelationalServicesBuilder(serviceCollection)
				.TryAddProviderSpecificServices(
					x => x.TryAddSingletonEnumerable<IRelationalTypeMappingSourcePlugin, MySqlConnectorNetTopologySuiteTypeMappingSourcePlugin>()
						.TryAddSingletonEnumerable<IMethodCallTranslatorPlugin, MySqlConnectorNetTopologySuiteMethodCallTranslatorPlugin>()
						.TryAddSingletonEnumerable<IMemberTranslatorPlugin, MySqlConnectorNetTopologySuiteMemberTranslatorPlugin>());

			return serviceCollection;
		}
	}
}
