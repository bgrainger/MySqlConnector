using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using MySqlConnector.EntityFrameworkCore.MySql.Scaffolding.Internal;
using MySqlConnector.EntityFrameworkCore.MySql.Storage.Internal;
using NetTopologySuite;

namespace MySqlConnector.EntityFrameworkCore.MySql.Design.Internal
{
	/// <summary>
	///     This API supports the Entity Framework Core infrastructure and is not intended to be used
	///     directly from your code. This API may change or be removed in future releases.
	/// </summary>
	public class MySqlConnectorNetTopologySuiteDesignTimeServices : IDesignTimeServices
	{
		/// <summary>
		///     This API supports the Entity Framework Core infrastructure and is not intended to be used
		///     directly from your code. This API may change or be removed in future releases.
		/// </summary>
		public virtual void ConfigureDesignTimeServices(IServiceCollection serviceCollection)
			=> serviceCollection
				.AddSingleton<IRelationalTypeMappingSourcePlugin, MySqlConnectorNetTopologySuiteTypeMappingSourcePlugin>()
				.AddSingleton<IProviderCodeGeneratorPlugin, MySqlConnectorNetTopologySuiteCodeGeneratorPlugin>()
				.TryAddSingleton(NtsGeometryServices.Instance);
	}
}
