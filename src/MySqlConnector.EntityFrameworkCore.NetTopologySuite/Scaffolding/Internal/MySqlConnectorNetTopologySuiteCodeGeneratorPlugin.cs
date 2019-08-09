using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;

namespace MySqlConnector.EntityFrameworkCore.MySql.Scaffolding.Internal
{
	/// <summary>
	///     This API supports the Entity Framework Core infrastructure and is not intended to be used
	///     directly from your code. This API may change or be removed in future releases.
	/// </summary>
	public class MySqlConnectorNetTopologySuiteCodeGeneratorPlugin : ProviderCodeGeneratorPlugin
	{
		/// <summary>
		///     This API supports the Entity Framework Core infrastructure and is not intended to be used
		///     directly from your code. This API may change or be removed in future releases.
		/// </summary>
		public override MethodCallCodeFragment GenerateProviderOptions()
			=> new MethodCallCodeFragment(
				nameof(MySqlConnectorNetTopologySuiteDbContextOptionsBuilderExtensions.UseNetTopologySuite));
	}
}
