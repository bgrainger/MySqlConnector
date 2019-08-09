using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore.Infrastructure;
using MySqlConnector.EntityFrameworkCore.MySql.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore;

namespace MySqlConnector.EntityFrameworkCore.MySql.Infrastructure.Internal
{
	/// <summary>
	///     This API supports the Entity Framework Core infrastructure and is not intended to be used
	///     directly from your code. This API may change or be removed in future releases.
	/// </summary>
	public class MySqlConnectorNetTopologySuiteOptionsExtension : IDbContextOptionsExtension
	{
		/// <summary>
		///     This API supports the Entity Framework Core infrastructure and is not intended to be used
		///     directly from your code. This API may change or be removed in future releases.
		/// </summary>
		public virtual string LogFragment => "using NetTopologySuite ";

		/// <summary>
		///     This API supports the Entity Framework Core infrastructure and is not intended to be used
		///     directly from your code. This API may change or be removed in future releases.
		/// </summary>
		public virtual bool ApplyServices(IServiceCollection services)
		{
			services.AddEntityFrameworkMySqlConnectorNetTopologySuite();

			return false;
		}

		/// <summary>
		///     This API supports the Entity Framework Core infrastructure and is not intended to be used
		///     directly from your code. This API may change or be removed in future releases.
		/// </summary>
		public virtual long GetServiceProviderHashCode() => 0;

		/// <summary>
		///     This API supports the Entity Framework Core infrastructure and is not intended to be used
		///     directly from your code. This API may change or be removed in future releases.
		/// </summary>
		public virtual void Validate(IDbContextOptions options)
		{
			var internalServiceProvider = options.FindExtension<CoreOptionsExtension>()?.InternalServiceProvider;
			if (internalServiceProvider != null)
			{
				using (var scope = internalServiceProvider.CreateScope())
				{
					if (scope.ServiceProvider.GetService<IEnumerable<IRelationalTypeMappingSourcePlugin>>()
							?.Any(s => s is MySqlConnectorNetTopologySuiteTypeMappingSourcePlugin) != true)
					{
						throw new InvalidOperationException($"{nameof(MySqlConnectorNetTopologySuiteDbContextOptionsBuilderExtensions.UseNetTopologySuite)} requires {nameof(MySqlConnectorNetTopologySuiteServiceCollectionExtensions.AddEntityFrameworkMySqlConnectorNetTopologySuite)} to be called on the internal service provider used.");
					}
				}
			}
		}
	}
}
