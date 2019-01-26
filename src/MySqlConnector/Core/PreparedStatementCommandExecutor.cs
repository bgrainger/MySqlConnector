using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Logging;
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Core
{
	internal sealed class PreparedStatementCommandExecutor : ICommandExecutor
	{
		public PreparedStatementCommandExecutor(MySqlCommand command, PreparedStatements preparedStatements)
		{
			m_command = command;
			m_preparedStatements = preparedStatements;
		}

		public async Task<DbDataReader> ExecuteReaderAsync(string commandText, MySqlParameterCollection parameterCollection, CommandBehavior behavior, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			cancellationToken.ThrowIfCancellationRequested();
			if (Log.IsDebugEnabled())
				Log.Debug("Session{0} ExecuteBehavior {1} CommandText: {2}", m_command.Connection.Session.Id, ioBehavior, commandText);
			using var payload = CreateQueryPayload(m_preparedStatements.Statements[0], parameterCollection, m_command.Connection.GuidFormat);
			using var _ = m_command.RegisterCancel(cancellationToken);
			m_command.Connection.Session.StartQuerying(m_command);
			m_command.LastInsertedId = -1;
			try
			{
				await m_command.Connection.Session.SendAsync(payload, ioBehavior, CancellationToken.None).ConfigureAwait(false);
				return await MySqlDataReader.CreateAsync(m_command, behavior, ResultSetProtocol.Binary, ioBehavior).ConfigureAwait(false);
			}
			catch (MySqlException ex) when (ex.Number == (int) MySqlErrorCode.QueryInterrupted && cancellationToken.IsCancellationRequested)
			{
				Log.Warn("Session{0} query was interrupted", m_command.Connection.Session.Id);
				throw new OperationCanceledException(cancellationToken);
			}
			catch (Exception ex) when (payload.ArraySegment.Count > 4_194_304 && (ex is SocketException || ex is IOException || ex is MySqlProtocolException))
			{
				// the default MySQL Server value for max_allowed_packet (in MySQL 5.7) is 4MiB: https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_allowed_packet
				// use "decimal megabytes" (to round up) when creating the exception message
				int megabytes = payload.ArraySegment.Count / 1_000_000;
				throw new MySqlException("Error submitting {0}MB packet; ensure 'max_allowed_packet' is greater than {0}MB.".FormatInvariant(megabytes), ex);
			}
		}

		private PayloadData CreateQueryPayload(PreparedStatement preparedStatement, MySqlParameterCollection parameterCollection, MySqlGuidFormat guidFormat)
		{
			var writer = new ByteBufferWriter();
			writer.Write((byte) CommandKind.StatementExecute);
			writer.Write(preparedStatement.StatementId);
			writer.Write((byte) 0);
			writer.Write(1);
			if (preparedStatement.Parameters?.Length > 0)
			{
				// TODO: How to handle incorrect number of parameters?

				// build subset of parameters for this statement
				var parameters = new MySqlParameter[preparedStatement.Statement.ParameterNames.Count];
				for (var i = 0; i < preparedStatement.Statement.ParameterNames.Count; i++)
				{
					var parameterName = preparedStatement.Statement.ParameterNames[i];
					var parameterIndex = parameterName != null ? (parameterCollection?.NormalizedIndexOf(parameterName) ?? -1) : preparedStatement.Statement.ParameterIndexes[i];
					if (parameterIndex == -1 && parameterName != null)
						throw new MySqlException("Parameter '{0}' must be defined.".FormatInvariant(parameterName));
					else if (parameterIndex < 0 || parameterIndex >= (parameterCollection?.Count ?? 0))
						throw new MySqlException("Parameter index {0} is invalid when only {1} parameter{2} defined.".FormatInvariant(parameterIndex, parameterCollection?.Count ?? 0, parameterCollection?.Count == 1 ? " is" : "s are"));
					parameters[i] = parameterCollection[parameterIndex];
				}

				// write null bitmap
				byte nullBitmap = 0;
				for (var i = 0; i < parameters.Length; i++)
				{
					var parameter = parameters[i];
					if (parameter.Value == null || parameter.Value == DBNull.Value)
						nullBitmap |= (byte) (1 << (i % 8));

					if (i % 8 == 7)
					{
						writer.Write(nullBitmap);
						nullBitmap = 0;
					}
				}
				if (parameters.Length % 8 != 0)
					writer.Write(nullBitmap);

				// write "new parameters bound" flag
				writer.Write((byte) 1);

				foreach (var parameter in parameters)
					writer.Write(TypeMapper.ConvertToColumnTypeAndFlags(parameter.MySqlDbType, guidFormat));

				var options = m_command.CreateStatementPreparerOptions();
				foreach (var parameter in parameters)
					parameter.AppendBinary(writer, options);
			}

			return writer.ToPayloadData();
		}

		static IMySqlConnectorLogger Log { get; } = MySqlConnectorLogManager.CreateLogger(nameof(PreparedStatementCommandExecutor));

		readonly MySqlCommand m_command;
		readonly PreparedStatements m_preparedStatements;
	}
}
