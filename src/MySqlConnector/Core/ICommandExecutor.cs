using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Protocol.Serialization;

namespace MySqlConnector.Core
{
	internal interface ICommandExecutor
	{
		Task<DbDataReader> ExecuteReaderAsync(string commandText, MySqlParameterCollection? parameterCollection, CommandBehavior behavior, IOBehavior ioBehavior, CancellationToken cancellationToken);
	}
}
