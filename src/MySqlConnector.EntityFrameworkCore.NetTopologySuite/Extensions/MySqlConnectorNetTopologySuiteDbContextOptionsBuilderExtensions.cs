using System;
using Microsoft.EntityFrameworkCore.Infrastructure;
using MySqlConnector.EntityFrameworkCore.MySql.Infrastructure.Internal;

namespace Microsoft.EntityFrameworkCore
{
	/// <summary>
	/// NetTopologySuite specific extension methods for <see cref="MySqlDbContextOptionsBuilder"/>.
	/// </summary>
	public static class MySqlConnectorNetTopologySuiteDbContextOptionsBuilderExtensions
	{
		/// <summary>
		///     Use NetTopologySuite to access SpatiaLite data.
		/// </summary>
		/// <param name="optionsBuilder">The builder being used to configure MySQL.</param>
		/// <returns>The options builder so that further configuration can be chained.</returns>
		public static MySqlDbContextOptionsBuilder UseNetTopologySuite(this MySqlDbContextOptionsBuilder optionsBuilder)
		{
			if (optionsBuilder is null)
				throw new ArgumentNullException(nameof(optionsBuilder));

			var coreOptionsBuilder = ((IRelationalDbContextOptionsBuilderInfrastructure)optionsBuilder).OptionsBuilder;
			var infrastructure = (IDbContextOptionsBuilderInfrastructure)coreOptionsBuilder;
			var ntsExtension = coreOptionsBuilder.Options.FindExtension<MySqlConnectorNetTopologySuiteOptionsExtension>()
				?? new MySqlConnectorNetTopologySuiteOptionsExtension();

			infrastructure.AddOrUpdateExtension(ntsExtension);

			return optionsBuilder;
		}
	}
}
