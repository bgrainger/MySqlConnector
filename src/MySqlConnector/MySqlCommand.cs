using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector.Core;
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector
{
	/// <summary>
	/// <see cref="MySqlCommand"/> represents a SQL statement or stored procedure name
	/// to execute against a MySQL database.
	/// </summary>
	public sealed class MySqlCommand : DbCommand, IMySqlCommand, ICancellableCommand
#if !NETSTANDARD1_3
		, ICloneable
#endif
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="MySqlCommand"/> class.
		/// </summary>
		public MySqlCommand()
			: this(null, null, null)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MySqlCommand"/> class, setting <see cref="CommandText"/> to <paramref name="commandText"/>.
		/// </summary>
		/// <param name="commandText">The text to assign to <see cref="CommandText"/>.</param>
		public MySqlCommand(string? commandText)
			: this(commandText, null, null)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MySqlCommand"/> class with the specified <see cref="MySqlConnection"/> and <see cref="MySqlTransaction"/>.
		/// </summary>
		/// <param name="connection">The <see cref="MySqlConnection"/> to use.</param>
		/// <param name="transaction">The active <see cref="MySqlTransaction"/>, if any.</param>
		public MySqlCommand(MySqlConnection? connection, MySqlTransaction? transaction)
			: this(null, connection, transaction)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MySqlCommand"/> class with the specified command text and <see cref="MySqlConnection"/>.
		/// </summary>
		/// <param name="commandText">The text to assign to <see cref="CommandText"/>.</param>
		/// <param name="connection">The <see cref="MySqlConnection"/> to use.</param>
		public MySqlCommand(string? commandText, MySqlConnection? connection)
			: this(commandText, connection, null)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MySqlCommand"/> class with the specified command text,<see cref="MySqlConnection"/>, and <see cref="MySqlTransaction"/>.
		/// </summary>
		/// <param name="commandText">The text to assign to <see cref="CommandText"/>.</param>
		/// <param name="connection">The <see cref="MySqlConnection"/> to use.</param>
		/// <param name="transaction">The active <see cref="MySqlTransaction"/>, if any.</param>
		public MySqlCommand(string? commandText, MySqlConnection? connection, MySqlTransaction? transaction)
		{
			GC.SuppressFinalize(this);
			m_commandId = ICancellableCommandExtensions.GetNextId();
			m_commandText = commandText ?? "";
			Connection = connection;
			Transaction = transaction;
			CommandType = CommandType.Text;
		}

		private MySqlCommand(MySqlCommand other)
			: this(other.CommandText, other.Connection, other.Transaction)
		{
			GC.SuppressFinalize(this);
			m_commandTimeout = other.m_commandTimeout;
			m_commandType = other.m_commandType;
			DesignTimeVisible = other.DesignTimeVisible;
			UpdatedRowSource = other.UpdatedRowSource;
			m_parameterCollection = other.CloneRawParameters();
		}

		/// <summary>
		/// The collection of <see cref="MySqlParameter"/> objects for this command.
		/// </summary>
		public new MySqlParameterCollection Parameters => m_parameterCollection ??= new();

		MySqlParameterCollection? IMySqlCommand.RawParameters => m_parameterCollection;

		public new MySqlParameter CreateParameter() => (MySqlParameter) base.CreateParameter();

		/// <inheritdoc/>
		public override void Cancel() => Connection?.Cancel(this);

		/// <inheritdoc/>
		public override int ExecuteNonQuery() => ExecuteNonQueryAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

		/// <inheritdoc/>
		public override object? ExecuteScalar() => ExecuteScalarAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

		public new MySqlDataReader ExecuteReader() => ExecuteReaderAsync(default, IOBehavior.Synchronous, default).GetAwaiter().GetResult();

		public new MySqlDataReader ExecuteReader(CommandBehavior commandBehavior) => ExecuteReaderAsync(commandBehavior, IOBehavior.Synchronous, default).GetAwaiter().GetResult();

		/// <inheritdoc/>
		public override void Prepare()
		{
			if (!NeedsPrepare(out var exception))
			{
				if (exception is not null)
					throw exception;
				return;
			}

			Connection!.Session.PrepareAsync(this, IOBehavior.Synchronous, default).GetAwaiter().GetResult();
		}

#if NET45 || NET461 || NET471 || NETSTANDARD1_3 || NETSTANDARD2_0 || NETCOREAPP2_1
		public Task PrepareAsync(CancellationToken cancellationToken = default) => PrepareAsync(AsyncIOBehavior, cancellationToken);
#else
		public override Task PrepareAsync(CancellationToken cancellationToken = default) => PrepareAsync(AsyncIOBehavior, cancellationToken);
#endif

		internal MySqlParameterCollection? CloneRawParameters()
		{
			if (m_parameterCollection is null)
				return null;
			var parameters = new MySqlParameterCollection();
			foreach (var parameter in (IEnumerable<MySqlParameter>) m_parameterCollection)
				parameters.Add(parameter.Clone());
			return parameters;
		}

		bool IMySqlCommand.AllowUserVariables => AllowUserVariables;

		internal bool AllowUserVariables { get; set; }

		private Task PrepareAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (!NeedsPrepare(out var exception))
				return exception is null ? Utility.CompletedTask : Utility.TaskFromException(exception);

			return Connection!.Session.PrepareAsync(this, ioBehavior, cancellationToken);
		}

		private bool NeedsPrepare(out Exception? exception)
		{
			exception = null;
			if (Connection is null)
				exception = new InvalidOperationException("Connection property must be non-null.");
			else if (Connection.State != ConnectionState.Open)
				exception = new InvalidOperationException("Connection must be Open; current state is {0}".FormatInvariant(Connection.State));
			else if (string.IsNullOrWhiteSpace(CommandText))
				exception = new InvalidOperationException("CommandText must be specified");
			else if (Connection?.HasActiveReader ?? false)
				exception = new InvalidOperationException("Cannot call Prepare when there is an open DataReader for this command's connection; it must be closed first.");

			if (exception is not null || Connection!.IgnorePrepare)
				return false;

			if (CommandType != CommandType.StoredProcedure && CommandType != CommandType.Text)
			{
				exception = new NotSupportedException("Only CommandType.Text and CommandType.StoredProcedure are currently supported by MySqlCommand.Prepare.");
				return false;
			}

			// don't prepare the same SQL twice
			return Connection.Session.TryGetPreparedStatement(CommandText!) is null;
		}

		/// <inheritdoc/>
		[AllowNull]
		public override string CommandText
		{
			get => m_commandText;
			set
			{
				if (m_connection?.ActiveCommandId == m_commandId)
					throw new InvalidOperationException("Cannot set MySqlCommand.CommandText when there is an open DataReader for this command; it must be closed first.");
				m_commandText = value ?? "";
			}
		}

		public bool IsPrepared => ((IMySqlCommand) this).TryGetPreparedStatements() is not null;

		public new MySqlTransaction? Transaction { get; set; }

		public new MySqlConnection? Connection
		{
			get => m_connection;
			set
			{
				if (m_connection?.ActiveCommandId == m_commandId)
					throw new InvalidOperationException("Cannot set MySqlCommand.Connection when there is an open DataReader for this command; it must be closed first.");
				m_connection = value;
			}
		}

		/// <inheritdoc/>
		public override int CommandTimeout
		{
			get => Math.Min(m_commandTimeout ?? Connection?.DefaultCommandTimeout ?? 0, int.MaxValue / 1000);
			set => m_commandTimeout = value >= 0 ? value : throw new ArgumentOutOfRangeException(nameof(value), "CommandTimeout must be greater than or equal to zero.");
		}

		/// <inheritdoc/>
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

		/// <inheritdoc/>
		public override bool DesignTimeVisible { get; set; }

		/// <inheritdoc/>
		public override UpdateRowSource UpdatedRowSource { get; set; }

		/// <summary>
		/// Holds the first automatically-generated ID for a value inserted in an <c>AUTO_INCREMENT</c> column in the last statement.
		/// </summary>
		/// <remarks>
		/// See <a href="https://dev.mysql.com/doc/refman/8.0/en/information-functions.html#function_last-insert-id"><c>LAST_INSERT_ID()</c></a> for more information.
		/// </remarks>
		public long LastInsertedId { get; private set; }

		void IMySqlCommand.SetLastInsertedId(long lastInsertedId) => LastInsertedId = lastInsertedId;

		protected override DbConnection? DbConnection
		{
			get => Connection;
			set => Connection = (MySqlConnection?) value;
		}

		protected override DbParameterCollection DbParameterCollection => Parameters;

		protected override DbTransaction? DbTransaction
		{
			get => Transaction;
			set => Transaction = (MySqlTransaction?) value;
		}

		protected override DbParameter CreateDbParameter() => new MySqlParameter();

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
			ExecuteReaderAsync(behavior, IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

		public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) =>
			ExecuteNonQueryAsync(AsyncIOBehavior, cancellationToken);

		internal async Task<int> ExecuteNonQueryAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			this.ResetCommandTimeout();
			using var registration = ((ICancellableCommand) this).RegisterCancel(cancellationToken);
			using var reader = await ExecuteReaderNoResetTimeoutAsync(CommandBehavior.Default, ioBehavior, cancellationToken).ConfigureAwait(false);
			do
			{
				while (await reader.ReadAsync(ioBehavior, cancellationToken).ConfigureAwait(false))
				{
				}
			} while (await reader.NextResultAsync(ioBehavior, cancellationToken).ConfigureAwait(false));
			return reader.RecordsAffected;
		}

		public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken) =>
			ExecuteScalarAsync(AsyncIOBehavior, cancellationToken);

		internal async Task<object?> ExecuteScalarAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			this.ResetCommandTimeout();
			using var registration = ((ICancellableCommand) this).RegisterCancel(cancellationToken);
			var hasSetResult = false;
			object? result = null;
			using var reader = await ExecuteReaderNoResetTimeoutAsync(CommandBehavior.Default, ioBehavior, cancellationToken).ConfigureAwait(false);
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
			return result;
		}

		public new Task<MySqlDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default) =>
			ExecuteReaderAsync(default, AsyncIOBehavior, cancellationToken);

		public new Task<MySqlDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default) =>
			ExecuteReaderAsync(behavior, AsyncIOBehavior, cancellationToken);

		protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
			await ExecuteReaderAsync(behavior, AsyncIOBehavior, cancellationToken).ConfigureAwait(false);

		internal async Task<MySqlDataReader> ExecuteReaderAsync(CommandBehavior behavior, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			this.ResetCommandTimeout();
			using var registration = ((ICancellableCommand) this).RegisterCancel(cancellationToken);
			return await ExecuteReaderNoResetTimeoutAsync(behavior, ioBehavior, cancellationToken).ConfigureAwait(false);
		}

		internal async Task<MySqlDataReader> ExecuteReaderNoResetTimeoutAsync(CommandBehavior behavior, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (!IsValid(out var exception))
				throw exception;

			if (((IMySqlCommand) this).TryGetPreparedStatements() is { Statements: { Count: 1 } } statements)
			{
				for (var i = 0; i < Parameters.Count; i++)
				{
					if (Parameters[i].Value is Stream stream)
					{
						var buffer = ArrayPool<byte>.Shared.Rent(1048576);
						int bytesRead;
						do
						{
							bytesRead = stream.Read(buffer, 0, buffer.Length);
							var writer = new ByteBufferWriter();

							writer.Write((byte) CommandKind.StatementSendLongData);
							writer.Write(statements.Statements[0].StatementId);
							writer.Write((ushort) i);
							writer.Write(buffer.AsSpan(0, bytesRead));
							await Connection!.Session.SendAsync(writer.ToPayloadData(), ioBehavior, cancellationToken).ConfigureAwait(false);
						}
						while (bytesRead > 0);
					}
				}
			}

			m_commandBehavior = behavior;
			return await CommandExecutor.ExecuteReaderAsync(new IMySqlCommand[] { this }, SingleCommandPayloadCreator.Instance, behavior, ioBehavior, cancellationToken).ConfigureAwait(false);
		}

		public MySqlCommand Clone() => new(this);

#if !NETSTANDARD1_3
		object ICloneable.Clone() => Clone();
#endif

		protected override void Dispose(bool disposing)
		{
			m_isDisposed = true;
			base.Dispose(disposing);
		}

#if NET45 || NET461 || NET471 || NETSTANDARD1_3 || NETSTANDARD2_0 || NETCOREAPP2_1
		public Task DisposeAsync()
#else
		public override ValueTask DisposeAsync()
#endif
		{
			Dispose();
#if NET45 || NET461 || NET471 || NETSTANDARD1_3 || NETSTANDARD2_0 || NETCOREAPP2_1
			return Utility.CompletedTask;
#else
			return default;
#endif
		}

		/// <summary>
		/// Registers <see cref="Cancel"/> as a callback with <paramref name="cancellationToken"/> if cancellation is supported.
		/// </summary>
		/// <param name="cancellationToken">The <see cref="CancellationToken"/>.</param>
		/// <returns>An object that must be disposed to revoke the cancellation registration.</returns>
		/// <remarks>This method is more efficient than calling <code>token.Register(Command.Cancel)</code> because it avoids
		/// unnecessary allocations.</remarks>
		IDisposable? ICancellableCommand.RegisterCancel(CancellationToken cancellationToken)
		{
			if (!cancellationToken.CanBeCanceled)
				return null;

			m_cancelAction ??= Cancel;
			return cancellationToken.Register(m_cancelAction);
		}

		void ICancellableCommand.SetTimeout(int milliseconds)
		{
			Volatile.Write(ref m_commandTimedOut, false);

			if (m_cancelTimerId != 0)
				TimerQueue.Instance.Remove(m_cancelTimerId);

			if (milliseconds != Constants.InfiniteTimeout)
			{
				m_cancelForCommandTimeoutAction ??= CancelCommandForTimeout;
				m_cancelTimerId = TimerQueue.Instance.Add(milliseconds, m_cancelForCommandTimeoutAction);
			}
		}

		bool ICancellableCommand.IsTimedOut => Volatile.Read(ref m_commandTimedOut);

		int ICancellableCommand.CommandId => m_commandId;

		int ICancellableCommand.CancelAttemptCount { get; set; }

		ICancellableCommand IMySqlCommand.CancellableCommand => this;

		private IOBehavior AsyncIOBehavior => Connection?.AsyncIOBehavior ?? IOBehavior.Asynchronous;

		private void CancelCommandForTimeout()
		{
			Volatile.Write(ref m_commandTimedOut, true);
			Cancel();
		}

		private bool IsValid([NotNullWhen(false)] out Exception? exception)
		{
			exception = null;
			if (m_isDisposed)
				exception = new ObjectDisposedException(GetType().Name);
			else if (Connection is null)
				exception = new InvalidOperationException("Connection property must be non-null.");
			else if (Connection.State != ConnectionState.Open && Connection.State != ConnectionState.Connecting)
				exception = new InvalidOperationException("Connection must be Open; current state is {0}".FormatInvariant(Connection.State));
			else if (!Connection.IgnoreCommandTransaction && Transaction != Connection.CurrentTransaction)
				exception = new InvalidOperationException("The transaction associated with this command is not the connection's active transaction; see https://fl.vu/mysql-trans");
			else if (string.IsNullOrWhiteSpace(CommandText))
				exception = new InvalidOperationException("CommandText must be specified");
			return exception is null;
		}

		PreparedStatements? IMySqlCommand.TryGetPreparedStatements() => CommandType == CommandType.Text && !string.IsNullOrWhiteSpace(CommandText) && m_connection is not null &&
			m_connection.State == ConnectionState.Open ? m_connection.Session.TryGetPreparedStatement(CommandText!) : null;

		CommandBehavior IMySqlCommand.CommandBehavior => m_commandBehavior;
		MySqlParameterCollection? IMySqlCommand.OutParameters { get; set; }
		MySqlParameter? IMySqlCommand.ReturnParameter { get; set; }

		readonly int m_commandId;
		bool m_isDisposed;
		MySqlConnection? m_connection;
		string m_commandText;
		MySqlParameterCollection? m_parameterCollection;
		int? m_commandTimeout;
		CommandType m_commandType;
		CommandBehavior m_commandBehavior;
		Action? m_cancelAction;
		Action? m_cancelForCommandTimeoutAction;
		uint m_cancelTimerId;
		bool m_commandTimedOut;
	}
}
