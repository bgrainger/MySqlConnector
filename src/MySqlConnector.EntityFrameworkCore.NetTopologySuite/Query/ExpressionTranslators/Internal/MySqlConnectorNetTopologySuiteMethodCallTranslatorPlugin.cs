using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Query.ExpressionTranslators;

namespace MySqlConnector.EntityFrameworkCore.MySql.Query.ExpressionTranslators.Internal
{
	/// <summary>
	///     This API supports the Entity Framework Core infrastructure and is not intended to be used
	///     directly from your code. This API may change or be removed in future releases.
	/// </summary>
	public class MySqlConnectorNetTopologySuiteMethodCallTranslatorPlugin : IMethodCallTranslatorPlugin
	{
		/// <summary>
		///     This API supports the Entity Framework Core infrastructure and is not intended to be used
		///     directly from your code. This API may change or be removed in future releases.
		/// </summary>
		public virtual IEnumerable<IMethodCallTranslator> Translators { get; } = new IMethodCallTranslator[]
		{
			new SqliteGeometryMethodTranslator(),
			new SqliteGeometryCollectionMethodTranslator(),
			new SqliteLineStringMethodTranslator(),
			new SqlitePolygonMethodTranslator()
		};
	}
}
