using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector.Core;
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Payloads;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySql.Data.MySqlClient
{
	public sealed class MySqlCommand : DbCommand
#if !NETSTANDARD1_3
		, ICloneable
#endif
	{
		public MySqlCommand()
			: this(null, null, null)
		{
		}

		public MySqlCommand(string commandText)
			: this(commandText, null, null)
		{
		}

		public MySqlCommand(MySqlConnection connection, MySqlTransaction transaction)
			: this(null, connection, transaction)
		{
		}

		public MySqlCommand(string commandText, MySqlConnection connection)
			: this(commandText, connection, null)
		{
		}

		public MySqlCommand(string commandText, MySqlConnection connection, MySqlTransaction transaction)
		{
			GC.SuppressFinalize(this);
			CommandId = Interlocked.Increment(ref s_commandId);
			CommandText = commandText;
			Connection = connection;
			Transaction = transaction;
			CommandType = CommandType.Text;
		}

		private MySqlCommand(MySqlCommand other)
			: this(other.CommandText, other.Connection, other.Transaction)
		{
			m_commandTimeout = other.m_commandTimeout;
			m_commandType = other.m_commandType;
			var parameters = Parameters;
			DesignTimeVisible = other.DesignTimeVisible;
			UpdatedRowSource = other.UpdatedRowSource;
			foreach (MySqlParameter parameter in other.Parameters)
				parameters.Add(parameter.Clone());
		}

		public new MySqlParameterCollection Parameters
		{
			get
			{
				VerifyNotDisposed();
				if (m_parameterCollection == null)
					m_parameterCollection = new MySqlParameterCollection();
				return m_parameterCollection;
			}
		}

		public new MySqlParameter CreateParameter() => (MySqlParameter) base.CreateParameter();

		public override void Cancel() => Connection.Cancel(this);

		public override int ExecuteNonQuery() => ExecuteNonQueryAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

		public override object ExecuteScalar() => ExecuteScalarAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

		public new MySqlDataReader ExecuteReader() => (MySqlDataReader) base.ExecuteReader();

		public new MySqlDataReader ExecuteReader(CommandBehavior commandBehavior) => (MySqlDataReader) base.ExecuteReader(commandBehavior);

		public override void Prepare()
		{
			if (!NeedsPrepare(out var exception))
			{
				if (exception != null)
					throw exception;
				return;
			}

			DoPrepareAsync(IOBehavior.Synchronous, default).GetAwaiter().GetResult();
		}

		public Task PrepareAsync() => PrepareAsync(AsyncIOBehavior, default);
		public Task PrepareAsync(CancellationToken cancellationToken) => PrepareAsync(AsyncIOBehavior, cancellationToken);

		private Task PrepareAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (!NeedsPrepare(out var exception))
				return exception != null ? Utility.TaskFromException(exception) : Utility.CompletedTask;

			return DoPrepareAsync(ioBehavior, cancellationToken);
		}

		private bool NeedsPrepare(out Exception exception)
		{
			exception = null;
			if (Connection == null)
				exception = new InvalidOperationException("Connection property must be non-null.");
			else if (Connection.State != ConnectionState.Open)
				exception = new InvalidOperationException("Connection must be Open; current state is {0}".FormatInvariant(Connection.State));
			else if (string.IsNullOrWhiteSpace(CommandText))
				exception = new InvalidOperationException("CommandText must be specified");
			else if (Connection?.HasActiveReader ?? false)
				exception = new InvalidOperationException("Cannot call Prepare when there is an open DataReader for this command; it must be closed first.");

			if (exception != null || Connection.IgnorePrepare)
				return false;

			if (CommandType != CommandType.Text)
			{
				exception = new NotSupportedException("Only CommandType.Text is currently supported by MySqlCommand.Prepare");
				return false;
			}

			// don't prepare the same SQL twice
			return Connection.Session.TryGetPreparedStatement(CommandText) == null;
		}

		private async Task DoPrepareAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			var statementPreparer = new StatementPreparer(CommandText, m_parameterCollection, CreateStatementPreparerOptions());
			var parsedStatements = statementPreparer.SplitStatements();

			if (parsedStatements.Statements.Count > 1)
				throw new NotSupportedException("Multiple semicolon-delimited SQL statements are not supported by MySqlCommand.Prepare");

			var columnsAndParameters = new ResizableArray<byte>();
			var columnsAndParametersSize = 0;

			var preparedStatements = new List<PreparedStatement>(parsedStatements.Statements.Count);
			foreach (var statement in parsedStatements.Statements)
			{
				await Connection.Session.SendAsync(new PayloadData(statement.StatementBytes), ioBehavior, cancellationToken).ConfigureAwait(false);
				var payload = await Connection.Session.ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				var response = StatementPrepareResponsePayload.Create(payload.AsSpan());

				ColumnDefinitionPayload[] parameters = null;
				if (response.ParameterCount > 0)
				{
					parameters = new ColumnDefinitionPayload[response.ParameterCount];
					for (var i = 0; i < response.ParameterCount; i++)
					{
						payload = await Connection.Session.ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
						Utility.Resize(ref columnsAndParameters, columnsAndParametersSize + payload.ArraySegment.Count);
						Buffer.BlockCopy(payload.ArraySegment.Array, payload.ArraySegment.Offset, columnsAndParameters.Array, columnsAndParametersSize, payload.ArraySegment.Count);
						parameters[i] = ColumnDefinitionPayload.Create(new ResizableArraySegment<byte>(columnsAndParameters, columnsAndParametersSize, payload.ArraySegment.Count));
						columnsAndParametersSize += payload.ArraySegment.Count;
					}
					if (!Connection.Session.SupportsDeprecateEof)
					{
						payload = await Connection.Session.ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
						EofPayload.Create(payload.AsSpan());
					}
				}

				ColumnDefinitionPayload[] columns = null;
				if (response.ColumnCount > 0)
				{
					columns = new ColumnDefinitionPayload[response.ColumnCount];
					for (var i = 0; i < response.ColumnCount; i++)
					{
						payload = await Connection.Session.ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
						Utility.Resize(ref columnsAndParameters, columnsAndParametersSize + payload.ArraySegment.Count);
						Buffer.BlockCopy(payload.ArraySegment.Array, payload.ArraySegment.Offset, columnsAndParameters.Array, columnsAndParametersSize, payload.ArraySegment.Count);
						columns[i] = ColumnDefinitionPayload.Create(new ResizableArraySegment<byte>(columnsAndParameters, columnsAndParametersSize, payload.ArraySegment.Count));
						columnsAndParametersSize += payload.ArraySegment.Count;
					}
					if (!Connection.Session.SupportsDeprecateEof)
					{
						payload = await Connection.Session.ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
						EofPayload.Create(payload.AsSpan());
					}
				}

				preparedStatements.Add(new PreparedStatement(response.StatementId, statement, columns, parameters));
			}

			Connection.Session.AddPreparedStatement(CommandText, new PreparedStatements(preparedStatements, parsedStatements));
		}

		public override string CommandText
		{
			get => m_commandText;
			set
			{
				if (m_connection?.HasActiveReader ?? false)
					throw new InvalidOperationException("Cannot set MySqlCommand.CommandText when there is an open DataReader for this command; it must be closed first.");
				m_commandText = value;
			}
		}

		public bool IsPrepared => TryGetPreparedStatement() != null;

		public new MySqlTransaction Transaction { get; set; }

		public new MySqlConnection Connection
		{
			get => m_connection;
			set
			{
				if (m_connection?.HasActiveReader ?? false)
					throw new InvalidOperationException("Cannot set MySqlCommand.Connection when there is an open DataReader for this command; it must be closed first.");
				m_connection = value;
			}
		}

		public override int CommandTimeout
		{
			get => Math.Min(m_commandTimeout ?? Connection?.DefaultCommandTimeout ?? 0, int.MaxValue / 1000);
			set => m_commandTimeout = value >= 0 ? value : throw new ArgumentOutOfRangeException(nameof(value), "CommandTimeout must be greater than or equal to zero.");
		}

		public override CommandType CommandType
		{
			get => m_commandType;
			set
			{
				if (value != CommandType.Text && value != CommandType.StoredProcedure)
					throw new ArgumentException("CommandType must be Text or StoredProcedure.", nameof(value));
				m_commandType = value;
			}
		}

		public override bool DesignTimeVisible { get; set; }

		public override UpdateRowSource UpdatedRowSource { get; set; }

		public long LastInsertedId { get; internal set; }

		protected override DbConnection DbConnection
		{
			get => Connection;
			set => Connection = (MySqlConnection) value;
		}

		protected override DbParameterCollection DbParameterCollection => Parameters;

		protected override DbTransaction DbTransaction
		{
			get => Transaction;
			set => Transaction = (MySqlTransaction) value;
		}

		protected override DbParameter CreateDbParameter()
		{
			VerifyNotDisposed();
			return new MySqlParameter();
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
		{
			ResetCommandTimeout();
			return ExecuteReaderAsync(behavior, IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();
		}

		public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) =>
			ExecuteNonQueryAsync(AsyncIOBehavior, cancellationToken);

		internal async Task<int> ExecuteNonQueryAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			ResetCommandTimeout();
			using (var reader = (MySqlDataReader) await ExecuteReaderAsync(CommandBehavior.Default, ioBehavior, cancellationToken).ConfigureAwait(false))
			{
				do
				{
					while (await reader.ReadAsync(ioBehavior, cancellationToken).ConfigureAwait(false))
					{
					}
				} while (await reader.NextResultAsync(ioBehavior, cancellationToken).ConfigureAwait(false));
				return reader.RecordsAffected;
			}
		}

		public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken) =>
			ExecuteScalarAsync(AsyncIOBehavior, cancellationToken);

		internal async Task<object> ExecuteScalarAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			ResetCommandTimeout();
			var hasSetResult = false;
			object result = null;
			using (var reader = (MySqlDataReader) await ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.SingleRow, ioBehavior, cancellationToken).ConfigureAwait(false))
			{
				do
				{
					var hasResult = await reader.ReadAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
					if (!hasSetResult)
					{
						if (hasResult)
							result = reader.GetValue(0);
						hasSetResult = true;
					}
				} while (await reader.NextResultAsync(ioBehavior, cancellationToken).ConfigureAwait(false));
			}
			return result;
		}

		protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
		{
			ResetCommandTimeout();
			return ExecuteReaderAsync(behavior, AsyncIOBehavior, cancellationToken);
		}

		internal Task<DbDataReader> ExecuteReaderAsync(CommandBehavior behavior, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (!IsValid(out var exception))
				return Utility.TaskFromException<DbDataReader>(exception);

			var preparedStatements = TryGetPreparedStatement();
			if (preparedStatements != null)
				m_commandExecutor = new PreparedStatementCommandExecutor(this, preparedStatements);
			else if (m_commandType == CommandType.Text)
				m_commandExecutor = new TextCommandExecutor(this);
			else if (m_commandType == CommandType.StoredProcedure)
				m_commandExecutor = new StoredProcedureCommandExecutor(this);

			return m_commandExecutor.ExecuteReaderAsync(CommandText, m_parameterCollection, behavior, ioBehavior, cancellationToken);
		}

		public MySqlCommand Clone() => new MySqlCommand(this);

#if !NETSTANDARD1_3
		object ICloneable.Clone() => Clone();
#endif

		protected override void Dispose(bool disposing)
		{
			try
			{
				if (disposing)
					m_parameterCollection = null;
			}
			finally
			{
				base.Dispose(disposing);
			}
			m_isDisposed = true;
		}

		/// <summary>
		/// Registers <see cref="Cancel"/> as a callback with <paramref name="token"/> if cancellation is supported.
		/// </summary>
		/// <param name="token">The <see cref="CancellationToken"/>.</param>
		/// <returns>An object that must be disposed to revoke the cancellation registration.</returns>
		/// <remarks>This method is more efficient than calling <code>token.Register(Command.Cancel)</code> because it avoids
		/// unnecessary allocations.</remarks>
		internal IDisposable RegisterCancel(CancellationToken token)
		{
			if (!token.CanBeCanceled)
				return null;

			if (m_cancelAction == null)
				m_cancelAction = Cancel;
			return token.Register(m_cancelAction);
		}

		internal int CommandId { get; }

		internal int CancelAttemptCount { get; set; }

		/// <summary>
		/// Causes the effective command timeout to be reset back to the value specified by <see cref="CommandTimeout"/>.
		/// </summary>
		/// <remarks>As per the <a href="https://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqlcommand.commandtimeout.aspx">MSDN documentation</a>,
		/// "This property is the cumulative time-out (for all network packets that are read during the invocation of a method) for all network reads during command
		/// execution or processing of the results. A time-out can still occur after the first row is returned, and does not include user processing time, only network
		/// read time. For example, with a 30 second time out, if Read requires two network packets, then it has 30 seconds to read both network packets. If you call
		/// Read again, it will have another 30 seconds to read any data that it requires."
		/// The <see cref="ResetCommandTimeout"/> method is called by public ADO.NET API methods to reset the effective time remaining at the beginning of a new
		/// method call.</remarks>
		internal void ResetCommandTimeout()
		{
			var commandTimeout = CommandTimeout;
			Connection?.Session?.SetTimeout(commandTimeout == 0 ? Constants.InfiniteTimeout : commandTimeout * 1000);
		}

		internal StatementPreparerOptions CreateStatementPreparerOptions()
		{
			var statementPreparerOptions = StatementPreparerOptions.None;
			if (Connection.AllowUserVariables || CommandType == CommandType.StoredProcedure)
				statementPreparerOptions |= StatementPreparerOptions.AllowUserVariables;
			if (Connection.DateTimeKind == DateTimeKind.Utc)
				statementPreparerOptions |= StatementPreparerOptions.DateTimeUtc;
			else if (Connection.DateTimeKind == DateTimeKind.Local)
				statementPreparerOptions |= StatementPreparerOptions.DateTimeLocal;
			if (CommandType == CommandType.StoredProcedure)
				statementPreparerOptions |= StatementPreparerOptions.AllowOutputParameters;

			statementPreparerOptions |= Connection.GuidFormat switch
			{
				MySqlGuidFormat.Char36 => StatementPreparerOptions.GuidFormatChar36,
				MySqlGuidFormat.Char32 => StatementPreparerOptions.GuidFormatChar32,
				MySqlGuidFormat.Binary16 => StatementPreparerOptions.GuidFormatBinary16,
				MySqlGuidFormat.TimeSwapBinary16 => StatementPreparerOptions.GuidFormatTimeSwapBinary16,
				MySqlGuidFormat.LittleEndianBinary16 => StatementPreparerOptions.GuidFormatLittleEndianBinary16,
				_ => StatementPreparerOptions.None
			};

			return statementPreparerOptions;
		}

		private IOBehavior AsyncIOBehavior => Connection?.AsyncIOBehavior ?? IOBehavior.Asynchronous;

		private void VerifyNotDisposed()
		{
			if (m_isDisposed)
				throw new ObjectDisposedException(GetType().Name);
		}

		private bool IsValid(out Exception exception)
		{
			exception = null;
			if (m_isDisposed)
				exception = new ObjectDisposedException(GetType().Name);
			else if (Connection == null)
				exception = new InvalidOperationException("Connection property must be non-null.");
			else if (Connection.State != ConnectionState.Open && Connection.State != ConnectionState.Connecting)
				exception = new InvalidOperationException("Connection must be Open; current state is {0}".FormatInvariant(Connection.State));
			else if (!Connection.IgnoreCommandTransaction && Transaction != Connection.CurrentTransaction)
				exception = new InvalidOperationException("The transaction associated with this command is not the connection's active transaction; see https://fl.vu/mysql-trans");
			else if (string.IsNullOrWhiteSpace(CommandText))
				exception = new InvalidOperationException("CommandText must be specified");
			return exception == null;
		}

		private PreparedStatements TryGetPreparedStatement() => CommandType == CommandType.Text && !string.IsNullOrWhiteSpace(CommandText) ? m_connection.Session.TryGetPreparedStatement(CommandText) : null;

		internal void ReaderClosed() => (m_commandExecutor as StoredProcedureCommandExecutor)?.SetParams();

		static int s_commandId = 1;

		bool m_isDisposed;
		MySqlConnection m_connection;
		string m_commandText;
		MySqlParameterCollection m_parameterCollection;
		int? m_commandTimeout;
		CommandType m_commandType;
		ICommandExecutor m_commandExecutor;
		Action m_cancelAction;
	}
}
