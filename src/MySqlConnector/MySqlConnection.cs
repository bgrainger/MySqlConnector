using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using MySqlConnector.Core;
using MySqlConnector.Logging;
using MySqlConnector.Protocol.Payloads;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector;

#if !NET6_0_OR_GREATER
#pragma warning disable CA1822 // Mark members as static
#endif

/// <summary>
/// <see cref="MySqlConnection"/> represents a connection to a MySQL database.
/// </summary>
public sealed class MySqlConnection : DbConnection, ICloneable
{
	public MySqlConnection()
		: this("")
	{
	}

	public MySqlConnection(string? connectionString)
		: this(connectionString ?? "", MySqlConnectorLoggingConfiguration.GlobalConfiguration)
	{
	}

	internal MySqlConnection(MySqlDataSource dataSource)
		: this(dataSource.ConnectionString, dataSource.LoggingConfiguration)
	{
		m_dataSource = dataSource;
	}

	private MySqlConnection(string connectionString, MySqlConnectorLoggingConfiguration loggingConfiguration)
	{
		GC.SuppressFinalize(this);
		m_connectionString = connectionString;
		LoggingConfiguration = loggingConfiguration;
		m_logger = loggingConfiguration.ConnectionLogger;
	}

#pragma warning disable CA2012 // Safe because method completes synchronously
	/// <summary>
	/// Begins a database transaction.
	/// </summary>
	/// <returns>A <see cref="MySqlTransaction"/> representing the new database transaction.</returns>
	/// <remarks>Transactions may not be nested.</remarks>
	public new MySqlTransaction BeginTransaction() => BeginTransactionAsync(IsolationLevel.Unspecified, default, IOBehavior.Synchronous, default).GetAwaiter().GetResult();

	/// <summary>
	/// Begins a database transaction.
	/// </summary>
	/// <param name="isolationLevel">The <see cref="IsolationLevel"/> for the transaction.</param>
	/// <returns>A <see cref="MySqlTransaction"/> representing the new database transaction.</returns>
	/// <remarks>Transactions may not be nested.</remarks>
	public new MySqlTransaction BeginTransaction(IsolationLevel isolationLevel) => BeginTransactionAsync(isolationLevel, default, IOBehavior.Synchronous, default).GetAwaiter().GetResult();

	/// <summary>
	/// Begins a database transaction.
	/// </summary>
	/// <param name="isolationLevel">The <see cref="IsolationLevel"/> for the transaction.</param>
	/// <param name="isReadOnly">If <c>true</c>, changes to tables used in the transaction are prohibited; otherwise, they are permitted.</param>
	/// <returns>A <see cref="MySqlTransaction"/> representing the new database transaction.</returns>
	/// <remarks>Transactions may not be nested.</remarks>
	public MySqlTransaction BeginTransaction(IsolationLevel isolationLevel, bool isReadOnly) => BeginTransactionAsync(isolationLevel, isReadOnly, IOBehavior.Synchronous, default).GetAwaiter().GetResult();

	/// <summary>
	/// Begins a database transaction.
	/// </summary>
	/// <param name="isolationLevel">The <see cref="IsolationLevel"/> for the transaction.</param>
	/// <returns>A <see cref="MySqlTransaction"/> representing the new database transaction.</returns>
	protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => BeginTransactionAsync(isolationLevel, default, IOBehavior.Synchronous, default).GetAwaiter().GetResult();
#pragma warning restore CA2012

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
	/// <summary>
	/// Begins a database transaction asynchronously.
	/// </summary>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="Task{MySqlTransaction}"/> representing the new database transaction.</returns>
	/// <remarks>Transactions may not be nested.</remarks>
	public new ValueTask<MySqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default) => BeginTransactionAsync(IsolationLevel.Unspecified, default, AsyncIOBehavior, cancellationToken);

	/// <summary>
	/// Begins a database transaction asynchronously.
	/// </summary>
	/// <param name="isolationLevel">The <see cref="IsolationLevel"/> for the transaction.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="Task{MySqlTransaction}"/> representing the new database transaction.</returns>
	/// <remarks>Transactions may not be nested.</remarks>
	public new ValueTask<MySqlTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default) => BeginTransactionAsync(isolationLevel, default, AsyncIOBehavior, cancellationToken);

	/// <summary>
	/// Begins a database transaction asynchronously.
	/// </summary>
	/// <param name="isolationLevel">The <see cref="IsolationLevel"/> for the transaction.</param>
	/// <param name="isReadOnly">If <c>true</c>, changes to tables used in the transaction are prohibited; otherwise, they are permitted.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="Task{MySqlTransaction}"/> representing the new database transaction.</returns>
	/// <remarks>Transactions may not be nested.</remarks>
	public ValueTask<MySqlTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, bool isReadOnly, CancellationToken cancellationToken = default) => BeginTransactionAsync(isolationLevel, isReadOnly, AsyncIOBehavior, cancellationToken);

	/// <summary>
	/// Begins a database transaction asynchronously.
	/// </summary>
	/// <param name="isolationLevel">The <see cref="IsolationLevel"/> for the transaction.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="ValueTask{DbTransaction}"/> representing the new database transaction.</returns>
	protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken) =>
		await BeginTransactionAsync(isolationLevel, default, AsyncIOBehavior, cancellationToken).ConfigureAwait(false);
#else
	/// <summary>
	/// Begins a database transaction asynchronously.
	/// </summary>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="Task{MySqlTransaction}"/> representing the new database transaction.</returns>
	/// <remarks>Transactions may not be nested.</remarks>
	public ValueTask<MySqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default) => BeginTransactionAsync(IsolationLevel.Unspecified, default, AsyncIOBehavior, cancellationToken);

	/// <summary>
	/// Begins a database transaction asynchronously.
	/// </summary>
	/// <param name="isolationLevel">The <see cref="IsolationLevel"/> for the transaction.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="Task{MySqlTransaction}"/> representing the new database transaction.</returns>
	/// <remarks>Transactions may not be nested.</remarks>
	public ValueTask<MySqlTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default) => BeginTransactionAsync(isolationLevel, default, AsyncIOBehavior, cancellationToken);

	/// <summary>
	/// Begins a database transaction asynchronously.
	/// </summary>
	/// <param name="isolationLevel">The <see cref="IsolationLevel"/> for the transaction.</param>
	/// <param name="isReadOnly">If <c>true</c>, changes to tables used in the transaction are prohibited; otherwise, they are permitted.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="Task{MySqlTransaction}"/> representing the new database transaction.</returns>
	/// <remarks>Transactions may not be nested.</remarks>
	public ValueTask<MySqlTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, bool isReadOnly, CancellationToken cancellationToken = default) => BeginTransactionAsync(isolationLevel, isReadOnly, AsyncIOBehavior, cancellationToken);
#endif

	private async ValueTask<MySqlTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, bool? isReadOnly, IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		if (State != ConnectionState.Open)
			throw new InvalidOperationException("Connection is not open.");
		if (CurrentTransaction is not null)
			throw new InvalidOperationException("Transactions may not be nested.");
		if (m_enlistedTransaction is not null)
			throw new InvalidOperationException("Cannot begin a transaction when already enlisted in a transaction.");

		var isolationLevelValue = isolationLevel switch
		{
			IsolationLevel.ReadUncommitted => "read uncommitted",
			IsolationLevel.ReadCommitted => "read committed",
			IsolationLevel.RepeatableRead => "repeatable read",
			IsolationLevel.Serializable => "serializable",
			IsolationLevel.Snapshot => "repeatable read",

			// "In terms of the SQL:1992 transaction isolation levels, the default InnoDB level is REPEATABLE READ." - http://dev.mysql.com/doc/refman/5.7/en/innodb-transaction-model.html
			IsolationLevel.Unspecified => "repeatable read",

			_ => throw new NotSupportedException($"IsolationLevel.{isolationLevel} is not supported."),
		};

		using (var cmd = new MySqlCommand($"set session transaction isolation level {isolationLevelValue};", this) { NoActivity = true })
		{
			await cmd.ExecuteNonQueryAsync(ioBehavior, cancellationToken).ConfigureAwait(false);

			var consistentSnapshotText = isolationLevel == IsolationLevel.Snapshot ? " with consistent snapshot" : "";
			var readOnlyText = isReadOnly switch
			{
				true => " read only",
				false => " read write",
				null => "",
			};
			var separatorText = (consistentSnapshotText.Length == 0 || readOnlyText.Length == 0) ? "" : ",";
			cmd.CommandText = $"start transaction{consistentSnapshotText}{separatorText}{readOnlyText};";
			await cmd.ExecuteNonQueryAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
		}

		var transaction = new MySqlTransaction(this, isolationLevel);
		CurrentTransaction = transaction;
		return transaction;
	}

	public override void EnlistTransaction(System.Transactions.Transaction? transaction)
	{
		if (State != ConnectionState.Open)
			throw new InvalidOperationException("Connection is not open.");

		// ignore reenlistment of same connection in same transaction
		if (m_enlistedTransaction?.Transaction.Equals(transaction) is true)
			return;

		if (m_enlistedTransaction is not null)
			throw new MySqlException("Already enlisted in a Transaction.");
		if (CurrentTransaction is not null)
			throw new InvalidOperationException("Can't enlist in a Transaction when there is an active MySqlTransaction.");

		if (transaction is not null)
		{
			var existingConnection = FindExistingEnlistedSession(transaction);
			if (existingConnection is not null)
			{
				// can reuse the existing connection
				CloseAsync(changeState: false, IOBehavior.Synchronous).GetAwaiter().GetResult();
				TakeSessionFrom(existingConnection);
				return;
			}
			else
			{
				m_enlistedTransaction = GetInitializedConnectionSettings().UseXaTransactions ?
					(EnlistedTransactionBase)new XaEnlistedTransaction(transaction, this) :
					new StandardEnlistedTransaction(transaction, this);
				m_enlistedTransaction.Start();

				lock (s_lock)
				{
					if (!s_transactionConnections.TryGetValue(transaction, out var enlistedTransactions))
						s_transactionConnections[transaction] = enlistedTransactions = new();
					enlistedTransactions.Add(m_enlistedTransaction);
				}
			}
		}
	}

	internal void UnenlistTransaction()
	{
		var transaction = m_enlistedTransaction!.Transaction;
		m_enlistedTransaction = null;

		// find this connection in the list of connections associated with the transaction
		bool? wasIdle = null;
		lock (s_lock)
		{
			var enlistedTransactions = s_transactionConnections[transaction];
			for (int i = 0; i < enlistedTransactions.Count; i++)
			{
				if (enlistedTransactions[i].Connection == this)
				{
					wasIdle = enlistedTransactions[i].IsIdle;
					enlistedTransactions.RemoveAt(i);
					break;
				}
			}
			if (enlistedTransactions.Count == 0)
				s_transactionConnections.Remove(transaction);
		}

		// if the connection was idle (i.e., the client already closed it), really close it now
		if (wasIdle is null)
			throw new InvalidOperationException("Didn't find transaction");
		if (wasIdle.Value)
			Close();
	}

	// If there is an idle (i.e., no client has it open) MySqlConnection thats part of 'transaction',
	// returns it; otherwise, returns null. If a valid MySqlConnection is returned, the current connection
	// has been stored in 's_transactionConnections' and the caller must call TakeSessionFrom to
	// transfer its session to this MySqlConnection.
	// Also performs validation checks to ensure that XA and non-XA transactions aren't being mixed.
	private MySqlConnection? FindExistingEnlistedSession(System.Transactions.Transaction transaction)
	{
		var hasEnlistedTransactions = false;
		var hasXaTransaction = false;
		lock (s_lock)
		{
			if (s_transactionConnections.TryGetValue(transaction, out var enlistedTransactions))
			{
				hasEnlistedTransactions = true;
				foreach (var enlistedTransaction in enlistedTransactions)
				{
					hasXaTransaction = enlistedTransaction.Connection.GetInitializedConnectionSettings().UseXaTransactions;
					if (enlistedTransaction.IsIdle && enlistedTransaction.Connection.m_connectionString == m_connectionString)
					{
						var existingConnection = enlistedTransaction.Connection;
						enlistedTransaction.Connection = this;
						enlistedTransaction.IsIdle = false;
						return existingConnection;
					}
				}
			}
		}

		// no valid existing connection was found; verify that constraints aren't violated
		if (GetInitializedConnectionSettings().UseXaTransactions)
		{
			if (hasEnlistedTransactions && !hasXaTransaction)
				throw new NotSupportedException("Cannot start an XA transaction when there is an existing non-XA transaction.");
		}
		else if (hasEnlistedTransactions)
		{
			throw new NotSupportedException("Multiple simultaneous connections or connections with different connection strings inside the same transaction are not supported when UseXaTransactions=False.");
		}
		return null;
	}

	private void TakeSessionFrom(MySqlConnection other)
	{
#if DEBUG
		if (other is null)
			throw new ArgumentNullException(nameof(other));
		if (m_session is not null)
			throw new InvalidOperationException("This connection must not have a session");
		if (other.m_session is null)
			throw new InvalidOperationException("Other connection must have a session");
		if (m_enlistedTransaction is not null)
			throw new InvalidOperationException("This connection must not have an enlisted transaction");
		if (other.m_enlistedTransaction is null)
			throw new InvalidOperationException("Other connection must have an enlisted transaction");
		if (m_activeReader is not null)
			throw new InvalidOperationException("This connection must not have an active reader");
		if (other.m_activeReader is not null)
			throw new InvalidOperationException("Other connection must not have an active reader");
#endif

		m_session = other.m_session;
		m_session!.OwningConnection = new(this);
		other.m_session = null;

		m_cachedProcedures = other.m_cachedProcedures;
		other.m_cachedProcedures = null;

		m_enlistedTransaction = other.m_enlistedTransaction;
		other.m_enlistedTransaction = null;
	}

	public override void Close() => CloseAsync(changeState: true, IOBehavior.Synchronous).GetAwaiter().GetResult();
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
	public override Task CloseAsync() => CloseAsync(changeState: true, SimpleAsyncIOBehavior);
#else
	public Task CloseAsync() => CloseAsync(changeState: true, SimpleAsyncIOBehavior);
#endif
	internal Task CloseAsync(IOBehavior ioBehavior) => CloseAsync(changeState: true, ioBehavior);

	public override void ChangeDatabase(string databaseName) => ChangeDatabaseAsync(IOBehavior.Synchronous, databaseName, CancellationToken.None).GetAwaiter().GetResult();
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
	public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default) => ChangeDatabaseAsync(AsyncIOBehavior, databaseName, cancellationToken);
#else
	public Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default) => ChangeDatabaseAsync(AsyncIOBehavior, databaseName, cancellationToken);
#endif

	private async Task ChangeDatabaseAsync(IOBehavior ioBehavior, string databaseName, CancellationToken cancellationToken)
	{
		if (string.IsNullOrWhiteSpace(databaseName))
			throw new ArgumentException("Database name is not valid.", nameof(databaseName));
		if (State != ConnectionState.Open)
			throw new InvalidOperationException("Connection is not open.");

		using (var initDatabasePayload = InitDatabasePayload.Create(databaseName))
			await m_session!.SendAsync(initDatabasePayload, ioBehavior, cancellationToken).ConfigureAwait(false);
		var payload = await m_session.ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
		OkPayload.Create(payload.Span, m_session.SupportsDeprecateEof, m_session.SupportsSessionTrack);
		m_session.DatabaseOverride = databaseName;
	}

	public new MySqlCommand CreateCommand() => (MySqlCommand)base.CreateCommand();

#pragma warning disable CA2012 // Safe because method completes synchronously
	public bool Ping() => PingAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();
#pragma warning restore CA2012
	public Task<bool> PingAsync(CancellationToken cancellationToken = default) => PingAsync(SimpleAsyncIOBehavior, cancellationToken).AsTask();

	private async ValueTask<bool> PingAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		if (m_session is null)
			return false;
		try
		{
			if (await m_session.TryPingAsync(logInfo: true, ioBehavior, cancellationToken).ConfigureAwait(false))
				return true;
		}
		catch (InvalidOperationException)
		{
		}

		SetState(ConnectionState.Closed);
		return false;
	}

	public override void Open() => OpenAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

	public override Task OpenAsync(CancellationToken cancellationToken) => OpenAsync(default, cancellationToken);

	internal async Task OpenAsync(IOBehavior? ioBehavior, CancellationToken cancellationToken)
	{
		var openStartTickCount = Environment.TickCount;

		VerifyNotDisposed();
		cancellationToken.ThrowIfCancellationRequested();
		if (State != ConnectionState.Closed)
			throw new InvalidOperationException($"Cannot Open when State is {State}.");

		using var activity = ActivitySourceHelper.StartActivity(ActivitySourceHelper.OpenActivityName);
		try
		{
			SetState(ConnectionState.Connecting);

			var pool = m_dataSource?.Pool ??
				ConnectionPool.GetPool(m_connectionString, LoggingConfiguration, createIfNotFound: true);
			m_connectionSettings ??= pool?.ConnectionSettings ?? new ConnectionSettings(new MySqlConnectionStringBuilder(m_connectionString));

			// check if there is an open session (in the current transaction) that can be adopted
			if (m_connectionSettings.AutoEnlist && System.Transactions.Transaction.Current is not null)
			{
				var existingConnection = FindExistingEnlistedSession(System.Transactions.Transaction.Current);
				if (existingConnection is not null)
				{
					TakeSessionFrom(existingConnection);
					ActivitySourceHelper.CopyTags(m_session!.ActivityTags, activity);
					m_hasBeenOpened = true;
					SetState(ConnectionState.Open);
					return;
				}
			}

			try
			{
				m_session = await CreateSessionAsync(pool, openStartTickCount, activity, ioBehavior, cancellationToken).ConfigureAwait(false);
				m_hasBeenOpened = true;
				SetState(ConnectionState.Open);
			}
			catch (OperationCanceledException ex)
			{
				SetState(ConnectionState.Closed);
				if (!cancellationToken.Equals(ex.CancellationToken))
					cancellationToken.ThrowIfCancellationRequested();
				throw;
			}
			catch (MySqlException)
			{
				SetState(ConnectionState.Closed);
				cancellationToken.ThrowIfCancellationRequested();
				throw;
			}
			catch (SocketException)
			{
				SetState(ConnectionState.Closed);
				throw new MySqlException(MySqlErrorCode.UnableToConnectToHost, "Unable to connect to any of the specified MySQL hosts.");
			}

			if (m_connectionSettings.AutoEnlist && System.Transactions.Transaction.Current is not null)
				EnlistTransaction(System.Transactions.Transaction.Current);

			activity?.SetSuccess();
		}
		catch (Exception ex) when (activity is { IsAllDataRequested: true })
		{
			// none of the other activity tags may have been set (depending on when the exception was thrown), so make sure at least the connection string is added, for diagnostics
			if (m_connectionSettings?.ConnectionStringBuilder is { } connectionStringBuilder)
				activity.SetTag(ActivitySourceHelper.DatabaseConnectionStringTagName, connectionStringBuilder.GetConnectionString(connectionStringBuilder.PersistSecurityInfo));
			activity.SetException(ex);
			throw;
		}
	}

	/// <summary>
	/// Resets the session state of the current open connection; this clears temporary tables and user-defined variables.
	/// </summary>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <c>ValueTask</c> representing the asynchronous operation.</returns>
	/// <remarks>This is an optional feature of the MySQL protocol and may not be supported by all servers.
	/// It's known to be supported by MySQL Server 5.7.3 (and later) and MariaDB 10.2.4 (and later).
	/// Other MySQL-compatible servers or proxies may not support this command.</remarks>
	public async ValueTask ResetConnectionAsync(CancellationToken cancellationToken = default)
	{
		var session = Session;
		Log.ResettingConnection(m_logger, session.Id);
		await session.SendAsync(ResetConnectionPayload.Instance, AsyncIOBehavior, cancellationToken).ConfigureAwait(false);
		var payload = await session.ReceiveReplyAsync(AsyncIOBehavior, cancellationToken).ConfigureAwait(false);
		OkPayload.Create(payload.Span, session.SupportsDeprecateEof, session.SupportsSessionTrack);
	}

	[AllowNull]
	public override string ConnectionString
	{
		get
		{
			if (!m_hasBeenOpened)
				return m_connectionString;
			var connectionStringBuilder = GetConnectionSettings().ConnectionStringBuilder;
			return connectionStringBuilder.GetConnectionString(connectionStringBuilder.PersistSecurityInfo);
		}
		set
		{
			if (m_connectionState == ConnectionState.Open)
				throw new InvalidOperationException("Cannot change the connection string on an open connection.");
			m_hasBeenOpened = false;
			m_connectionString = value ?? "";
			m_connectionSettings = null;
		}
	}

	public override string Database => m_session?.DatabaseOverride ?? GetConnectionSettings().Database;

	public override ConnectionState State => m_connectionState;

	public override string DataSource => GetConnectionSettings().ConnectionStringBuilder.Server;

	public override string ServerVersion => Session.ServerVersion.OriginalString;

	/// <summary>
	/// The connection ID from MySQL Server.
	/// </summary>
	public int ServerThread => Session.ConnectionId;

	/// <summary>
	/// Gets or sets the delegate used to provide client certificates for connecting to a server.
	/// </summary>
	/// <remarks>The provided <see cref="X509CertificateCollection"/> should be filled with the client certificate(s) needed to connect to the server.</remarks>
	public Func<X509CertificateCollection, ValueTask>? ProvideClientCertificatesCallback { get; set; }

	/// <summary>
	/// Gets or sets the delegate used to generate a password for new database connections.
	/// </summary>
	/// <remarks>
	/// <para>This delegate is executed when a new database connection is opened that requires a password. Due to
	/// connection pooling, this delegate is only executed when a new physical connection is established with a database
	/// server, not when a connection is retrieved from the pool.</para>
	/// <para>The <see cref="MySqlConnectionStringBuilder.Password"/> option takes precedence over this
	/// delegate if it is specified.</para>
	/// <para>Using this delegate can make more efficient use of connection pooling for servers that require
	/// frequently-changing passwords or authentication tokens. Changing the password in the connection string
	/// will create unique connection pools; this delegate allows a single connection pool to use multiple passwords.</para>
	/// </remarks>
	public Func<MySqlProvidePasswordContext, string>? ProvidePasswordCallback { get; set; }

	/// <summary>
	/// Gets or sets the delegate used to verify that the server's certificate is valid.
	/// </summary>
	/// <remarks><see cref="MySqlConnectionStringBuilder.SslMode"/> must be set to <see cref="MySqlSslMode.Preferred"/>
	/// or <see cref="MySqlSslMode.Required"/> in order for this delegate to be invoked. See the documentation for
	/// <see cref="RemoteCertificateValidationCallback"/> for more information on the values passed to this delegate.</remarks>
	public RemoteCertificateValidationCallback? RemoteCertificateValidationCallback { get; set; }

	/// <summary>
	/// Clears the connection pool that <paramref name="connection"/> belongs to.
	/// </summary>
	/// <param name="connection">The <see cref="MySqlConnection"/> whose connection pool will be cleared.</param>
	public static void ClearPool(MySqlConnection connection) => ClearPoolAsync(connection, IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

	/// <summary>
	/// Asynchronously clears the connection pool that <paramref name="connection"/> belongs to.
	/// </summary>
	/// <param name="connection">The <see cref="MySqlConnection"/> whose connection pool will be cleared.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
	public static Task ClearPoolAsync(MySqlConnection connection, CancellationToken cancellationToken = default) => ClearPoolAsync(connection, connection.AsyncIOBehavior, cancellationToken);

	/// <summary>
	/// Clears all connection pools.
	/// </summary>
	public static void ClearAllPools() => ConnectionPool.ClearPoolsAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

	/// <summary>
	/// Asynchronously clears all connection pools.
	/// </summary>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
	public static Task ClearAllPoolsAsync(CancellationToken cancellationToken = default) => ConnectionPool.ClearPoolsAsync(IOBehavior.Asynchronous, cancellationToken);

	private static async Task ClearPoolAsync(MySqlConnection connection, IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		if (connection is null)
			throw new ArgumentNullException(nameof(connection));

		var pool = ConnectionPool.GetPool(connection.m_connectionString, null, createIfNotFound: false);
		if (pool is not null)
			await pool.ClearAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
	}

	protected override DbCommand CreateDbCommand() => new MySqlCommand(this, null);

	protected override DbProviderFactory DbProviderFactory => MySqlConnectorFactory.Instance;

#pragma warning disable CA2012 // Safe because method completes synchronously
	/// <summary>
	/// Returns schema information for the data source of this <see cref="MySqlConnection"/>.
	/// </summary>
	/// <returns>A <see cref="DataTable"/> containing schema information.</returns>
	public override DataTable GetSchema() => GetSchemaProvider().GetSchemaAsync(IOBehavior.Synchronous, "MetaDataCollections", default, default).GetAwaiter().GetResult();

	/// <summary>
	/// Returns schema information for the data source of this <see cref="MySqlConnection"/>.
	/// </summary>
	/// <param name="collectionName">The name of the schema to return. See <a href="https://mysqlconnector.net/overview/schema-collections/">Supported Schema Collections</a> for the list of supported schema names.</param>
	/// <returns>A <see cref="DataTable"/> containing schema information.</returns>
	public override DataTable GetSchema(string collectionName) => GetSchemaProvider().GetSchemaAsync(IOBehavior.Synchronous, collectionName, default, default).GetAwaiter().GetResult();

	/// <summary>
	/// Returns schema information for the data source of this <see cref="MySqlConnection"/>.
	/// </summary>
	/// <param name="collectionName">The name of the schema to return. See <a href="https://mysqlconnector.net/overview/schema-collections/">Supported Schema Collections</a> for the list of supported schema names.</param>
	/// <param name="restrictionValues">The restrictions to apply to the schema.</param>
	/// <returns>A <see cref="DataTable"/> containing schema information.</returns>
	public override DataTable GetSchema(string collectionName, string?[] restrictionValues) => GetSchemaProvider().GetSchemaAsync(IOBehavior.Synchronous, collectionName, restrictionValues, default).GetAwaiter().GetResult();
#pragma warning restore CA2012

	/// <summary>
	/// Asynchronously returns schema information for the data source of this <see cref="MySqlConnection"/>.
	/// </summary>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="Task{DataTable}"/> containing schema information.</returns>
	/// <remarks>The proposed ADO.NET API that this is based on is not finalized; this API may change in the future.</remarks>
#if NET5_0_OR_GREATER
	public override Task<DataTable> GetSchemaAsync(CancellationToken cancellationToken = default)
#else
	public Task<DataTable> GetSchemaAsync(CancellationToken cancellationToken = default)
#endif
		=> GetSchemaProvider().GetSchemaAsync(AsyncIOBehavior, "MetaDataCollections", default, cancellationToken).AsTask();

	/// <summary>
	/// Asynchronously returns schema information for the data source of this <see cref="MySqlConnection"/>.
	/// </summary>
	/// <param name="collectionName">The name of the schema to return.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="Task{DataTable}"/> containing schema information.</returns>
	/// <remarks>The proposed ADO.NET API that this is based on is not finalized; this API may change in the future.</remarks>
#if NET5_0_OR_GREATER
	public override Task<DataTable> GetSchemaAsync(string collectionName, CancellationToken cancellationToken = default)
#else
	public Task<DataTable> GetSchemaAsync(string collectionName, CancellationToken cancellationToken = default)
#endif
		=> GetSchemaProvider().GetSchemaAsync(AsyncIOBehavior, collectionName, default, cancellationToken).AsTask();

	/// <summary>
	/// Asynchronously returns schema information for the data source of this <see cref="MySqlConnection"/>.
	/// </summary>
	/// <param name="collectionName">The name of the schema to return.</param>
	/// <param name="restrictionValues">The restrictions to apply to the schema.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A <see cref="Task{DataTable}"/> containing schema information.</returns>
	/// <remarks>The proposed ADO.NET API that this is based on is not finalized; this API may change in the future.</remarks>
#if NET5_0_OR_GREATER
	public override Task<DataTable> GetSchemaAsync(string collectionName, string?[] restrictionValues, CancellationToken cancellationToken = default)
#else
	public Task<DataTable> GetSchemaAsync(string collectionName, string?[] restrictionValues, CancellationToken cancellationToken = default)
#endif
		=> GetSchemaProvider().GetSchemaAsync(AsyncIOBehavior, collectionName, restrictionValues, cancellationToken).AsTask();

	private SchemaProvider GetSchemaProvider() => m_schemaProvider ??= new(this);

	/// <summary>
	/// Gets the time (in seconds) to wait while trying to establish a connection
	/// before terminating the attempt and generating an error. This value
	/// is controlled by <see cref="MySqlConnectionStringBuilder.ConnectionTimeout"/>,
	/// which defaults to 15 seconds.
	/// </summary>
	public override int ConnectionTimeout => GetConnectionSettings().ConnectionTimeout;

	public event MySqlInfoMessageEventHandler? InfoMessage;

	/// <summary>
	/// Creates a <see cref="MySqlBatch"/> object for executing batched commands.
	/// </summary>
#if NET6_0_OR_GREATER
	public new MySqlBatch CreateBatch() => new(this);
	protected override DbBatch CreateDbBatch() => CreateBatch();
	public override bool CanCreateBatch => true;
#else
	public MySqlBatch CreateBatch() => new(this);
	public bool CanCreateBatch => true;
#endif

	protected override void Dispose(bool disposing)
	{
		try
		{
			if (disposing)
				CloseAsync(changeState: true, IOBehavior.Synchronous).GetAwaiter().GetResult();
		}
		finally
		{
			m_isDisposed = true;
			base.Dispose(disposing);
		}
	}

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
	public override async ValueTask DisposeAsync()
#else
	public async Task DisposeAsync()
#endif
	{
		try
		{
			await CloseAsync(changeState: true, SimpleAsyncIOBehavior).ConfigureAwait(false);
		}
		finally
		{
			m_isDisposed = true;

			// Component implements the Dispose pattern, with some core logic implemented in Dispose(bool disposing). DbConnection
			// adds DisposeAsync but doesn't implement the full DisposeAsyncCore pattern. Thus, although DisposeAsync is supposed
			// to call Dispose(false), we call Dispose(true) here to execute that base class logic in both the sync and async paths.
			base.Dispose(true);
		}
	}

	public MySqlConnection Clone() => new(this, m_dataSource, m_connectionString, m_hasBeenOpened);

	object ICloneable.Clone() => Clone();

	/// <summary>
	/// Returns an unopened copy of this connection with a new connection string. If the <c>Password</c>
	/// in <paramref name="connectionString"/> is not set, the password from this connection will be used.
	/// This allows creating a new connection with the same security information while changing other options,
	/// such as database or pooling.
	/// </summary>
	/// <param name="connectionString">The new connection string to be used.</param>
	/// <returns>A new <see cref="MySqlConnection"/> with different connection string options but
	/// the same password as this connection (unless overridden by <paramref name="connectionString"/>).</returns>
	public MySqlConnection CloneWith(string connectionString)
	{
		var newBuilder = new MySqlConnectionStringBuilder(connectionString ?? throw new ArgumentNullException(nameof(connectionString)));
		var currentBuilder = GetConnectionSettings().ConnectionStringBuilder;
		var shouldCopyPassword = newBuilder.Password.Length == 0 && (!newBuilder.PersistSecurityInfo || currentBuilder.PersistSecurityInfo);
		if (shouldCopyPassword)
			newBuilder.Password = currentBuilder.Password;
		var newConnectionString = newBuilder.ConnectionString;
		var dataSource = newConnectionString == currentBuilder.ConnectionString ? m_dataSource : null;
		return new MySqlConnection(this, dataSource, newConnectionString, m_hasBeenOpened && shouldCopyPassword && !currentBuilder.PersistSecurityInfo);
	}

	internal ServerSession Session
	{
		get
		{
			VerifyNotDisposed();
			if (m_session is null || State != ConnectionState.Open)
				throw new InvalidOperationException($"Connection must be Open; current state is {State}");
			return m_session;
		}
	}

	internal void SetSessionFailed(Exception exception) => m_session!.SetFailed(exception);

	internal void Cancel(ICancellableCommand command, int commandId, bool isCancel)
	{
		if (m_session?.Id is not string sessionId || State != ConnectionState.Open || m_session?.TryStartCancel(command) is not true)
		{
			Log.IgnoringCancellationForCommand(m_logger, commandId);
			return;
		}

		Log.CommandHasBeenCanceled(m_logger, commandId, sessionId, isCancel ? "Cancel()" : "command timeout");
		try
		{
			// open a dedicated connection to the server to kill the active query
			var csb = new MySqlConnectionStringBuilder(m_connectionString)
			{
				AutoEnlist = false,
				Pooling = false,
			};
			if (m_session.IPEndPoint is { Address: { } ipAddress, Port: { } port })
			{
				csb.Server = ipAddress.ToString();
				csb.Port = (uint) port;
			}
			csb.UserID = m_session.UserID;
			var cancellationTimeout = GetConnectionSettings().CancellationTimeout;
			csb.ConnectionTimeout = cancellationTimeout < 1 ? 3u : (uint) cancellationTimeout;

			using var connection = CloneWith(csb.ConnectionString);
			connection.Open();
#if NET6_0_OR_GREATER
			var killQuerySql = string.Create(CultureInfo.InvariantCulture, $"KILL QUERY {command.Connection!.ServerThread}");
#else
			var killQuerySql = FormattableString.Invariant($"KILL QUERY {command.Connection!.ServerThread}");
#endif
			using var killCommand = new MySqlCommand(killQuerySql, connection);
			killCommand.CommandTimeout = cancellationTimeout < 1 ? 3 : cancellationTimeout;
			m_session?.DoCancel(command, killCommand);
		}
		catch (InvalidOperationException ex)
		{
			// ignore a rare race condition where the connection is open at the beginning of the method, but closed by the time
			// KILL QUERY is executed: https://github.com/mysql-net/MySqlConnector/issues/1002
			Log.IgnoringCancellationForClosedConnection(m_logger, ex, sessionId);
			m_session?.AbortCancel(command);
		}
		catch (MySqlException ex)
		{
			// cancelling the query failed; setting the state back to 'Querying' will allow another call to 'Cancel' to try again
			Log.CancelingCommandFailed(m_logger, ex, sessionId, command.CommandId);
			m_session?.AbortCancel(command);
		}
	}

	internal async Task<CachedProcedure?> GetCachedProcedure(string name, bool revalidateMissing, IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		Log.GettingCachedProcedure(m_logger, m_session!.Id, name);
		if (State != ConnectionState.Open)
			throw new InvalidOperationException("Connection is not open.");

		var cachedProcedures = m_session!.Pool?.GetProcedureCache() ?? m_cachedProcedures;
		if (cachedProcedures is null)
		{
			Log.PoolDoesNotHaveSharedProcedureCache(m_logger, m_session.Id, m_session.Pool?.Id);
			cachedProcedures = m_cachedProcedures = new();
		}

		var normalized = NormalizedSchema.MustNormalize(name, Database);
		if (string.IsNullOrEmpty(normalized.Schema))
		{
			Log.CouldNotNormalizeDatabaseAndName(m_logger, m_session.Id, name, Database);
			return null;
		}

		CachedProcedure? cachedProcedure;
		bool foundProcedure;
		lock (cachedProcedures)
			foundProcedure = cachedProcedures.TryGetValue(normalized.FullyQualified, out cachedProcedure);
		if (!foundProcedure || (cachedProcedure is null && revalidateMissing))
		{
			cachedProcedure = await CachedProcedure.FillAsync(ioBehavior, this, normalized.Schema!, normalized.Component!, m_logger, cancellationToken).ConfigureAwait(false);
			if (cachedProcedure is null)
				Log.FailedToCacheProcedure(m_logger, m_session.Id, normalized.Schema!, normalized.Component!);
			else
				Log.CachingProcedure(m_logger, m_session.Id, normalized.Schema!, normalized.Component!);
			int count;
			lock (cachedProcedures)
			{
				cachedProcedures[normalized.FullyQualified] = cachedProcedure;
				count = cachedProcedures.Count;
			}
			Log.ProcedureCacheCount(m_logger, m_session.Id, count);
		}

		if (cachedProcedure is null)
			Log.DidNotFindCachedProcedure(m_logger, m_session.Id, normalized.Schema!, normalized.Component!);
		else
			Log.ReturningCachedProcedure(m_logger, m_session.Id, normalized.Schema!, normalized.Component!);
		return cachedProcedure;
	}

	internal MySqlTransaction? CurrentTransaction { get; set; }
	internal MySqlConnectorLoggingConfiguration LoggingConfiguration { get; }
	internal bool AllowLoadLocalInfile => GetInitializedConnectionSettings().AllowLoadLocalInfile;
	internal bool AllowUserVariables => GetInitializedConnectionSettings().AllowUserVariables;
	internal bool AllowZeroDateTime => GetInitializedConnectionSettings().AllowZeroDateTime;
	internal bool ConvertZeroDateTime => GetInitializedConnectionSettings().ConvertZeroDateTime;
	internal DateTimeKind DateTimeKind => GetInitializedConnectionSettings().DateTimeKind;
	internal int DefaultCommandTimeout => GetConnectionSettings().DefaultCommandTimeout;
	internal MySqlGuidFormat GuidFormat => GetInitializedConnectionSettings().GuidFormat;
	internal bool IgnoreCommandTransaction => GetInitializedConnectionSettings().IgnoreCommandTransaction || m_enlistedTransaction is StandardEnlistedTransaction;
	internal bool IgnorePrepare => GetInitializedConnectionSettings().IgnorePrepare;
	internal bool NoBackslashEscapes => GetInitializedConnectionSettings().NoBackslashEscapes;
	internal bool TreatTinyAsBoolean => GetInitializedConnectionSettings().TreatTinyAsBoolean;
	internal IOBehavior AsyncIOBehavior => GetConnectionSettings().ForceSynchronous ? IOBehavior.Synchronous : IOBehavior.Asynchronous;

	// Defaults to IOBehavior.Synchronous if the connection hasn't been opened yet; only use if it's a no-op for a closed connection.
	internal IOBehavior SimpleAsyncIOBehavior => (m_connectionSettings?.ForceSynchronous is true) ? IOBehavior.Synchronous : IOBehavior.Asynchronous;

	internal MySqlSslMode SslMode => GetInitializedConnectionSettings().SslMode;

	internal int? ActiveCommandId => m_session?.ActiveCommandId;

	internal bool SupportsPerQueryVariables => m_session?.SupportsPerQueryVariables ?? false;
	internal bool HasActiveReader => m_activeReader is not null;

	internal void SetActiveReader(MySqlDataReader dataReader)
	{
		if (dataReader is null)
			throw new ArgumentNullException(nameof(dataReader));
		if (m_activeReader is not null)
			throw new InvalidOperationException("Can't replace active reader.");
		m_activeReader = dataReader;
	}

	internal void FinishQuerying(bool hasWarnings)
	{
		m_session!.FinishQuerying();
		m_activeReader = null;

		if (hasWarnings && InfoMessage is not null)
		{
			var errors = new List<MySqlError>();
			using (var command = new MySqlCommand("SHOW WARNINGS;", this))
			{
				command.Transaction = CurrentTransaction;
				using var reader = command.ExecuteReader();
				while (reader.Read())
					errors.Add(new(reader.GetString(0), reader.GetInt32(1), reader.GetString(2)));
			}

			InfoMessage(this, new MySqlInfoMessageEventArgs(errors));
		}
	}

	private async ValueTask<ServerSession> CreateSessionAsync(ConnectionPool? pool, int startTickCount, Activity? activity, IOBehavior? ioBehavior, CancellationToken cancellationToken)
	{
		pool?.AddPendingRequestCount(1);
		var connectionSettings = GetInitializedConnectionSettings();
		var actualIOBehavior = ioBehavior ?? (connectionSettings.ForceSynchronous ? IOBehavior.Synchronous : IOBehavior.Asynchronous);

		CancellationTokenSource? timeoutSource = null;
		CancellationTokenSource? linkedSource = null;
		try
		{
			// the cancellation token for connection is controlled by 'cancellationToken' (if it can be cancelled), ConnectionTimeout
			// (from the connection string, if non-zero), or a combination of both
			if (connectionSettings.ConnectionTimeout != 0)
				timeoutSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(Math.Max(1, connectionSettings.ConnectionTimeoutMilliseconds - unchecked(Environment.TickCount - startTickCount))));
			if (cancellationToken.CanBeCanceled && timeoutSource is not null)
				linkedSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);
			var connectToken = linkedSource?.Token ?? timeoutSource?.Token ?? cancellationToken;

			// get existing session from the pool if possible
			if (pool is not null)
			{
				// this returns an open session
				return await pool.GetSessionAsync(this, startTickCount, activity, actualIOBehavior, connectToken).ConfigureAwait(false);
			}
			else
			{
				// only "fail over" and "random" load balancers supported without connection pooling
				var loadBalancer = connectionSettings.LoadBalance == MySqlLoadBalance.Random && connectionSettings.HostNames!.Count > 1 ?
					RandomLoadBalancer.Instance : FailOverLoadBalancer.Instance;

				var session = new ServerSession(m_logger);
				session.OwningConnection = new WeakReference<MySqlConnection>(this);
				Log.CreatedNonPooledSession(m_logger, session.Id);
				try
				{
					await session.ConnectAsync(connectionSettings, this, startTickCount, loadBalancer, activity, actualIOBehavior, connectToken).ConfigureAwait(false);
					return session;
				}
				catch (Exception)
				{
					await session.DisposeAsync(actualIOBehavior, default).ConfigureAwait(false);
					throw;
				}
			}
		}
		catch (OperationCanceledException) when (timeoutSource?.IsCancellationRequested is true)
		{
			var messageSuffix = (pool?.IsEmpty is true) ? " All pooled connections are in use." : "";
			throw new MySqlException(MySqlErrorCode.UnableToConnectToHost, "Connect Timeout expired." + messageSuffix);
		}
		catch (MySqlException ex) when ((timeoutSource?.IsCancellationRequested is true) || (ex.ErrorCode == MySqlErrorCode.CommandTimeoutExpired))
		{
			throw new MySqlException(MySqlErrorCode.UnableToConnectToHost, "Connect Timeout expired.", ex);
		}
		finally
		{
			pool?.AddPendingRequestCount(-1);
			linkedSource?.Dispose();
			timeoutSource?.Dispose();
		}
	}

	internal bool SslIsEncrypted => m_session!.SslIsEncrypted;

	internal bool SslIsSigned => m_session!.SslIsSigned;

	internal bool SslIsAuthenticated => m_session!.SslIsAuthenticated;

	internal bool SslIsMutuallyAuthenticated => m_session!.SslIsMutuallyAuthenticated;

	internal SslProtocols SslProtocol => m_session!.SslProtocol;

	internal void SetState(ConnectionState newState)
	{
		if (m_connectionState != newState)
		{
			var previousState = m_connectionState;
			m_connectionState = newState;
			var eventArgs =
				previousState == ConnectionState.Closed && newState == ConnectionState.Connecting ? s_stateChangeClosedConnecting :
				previousState == ConnectionState.Connecting && newState == ConnectionState.Open ? s_stateChangeConnectingOpen :
				previousState == ConnectionState.Open && newState == ConnectionState.Closed ? s_stateChangeOpenClosed :
				new(previousState, newState);
			OnStateChange(eventArgs);
		}
	}

	private MySqlConnection(MySqlConnection other, MySqlDataSource? dataSource, string connectionString, bool hasBeenOpened)
		: this(connectionString, other.LoggingConfiguration)
	{
		m_dataSource = dataSource;
		m_hasBeenOpened = hasBeenOpened;
		ProvideClientCertificatesCallback = other.ProvideClientCertificatesCallback;
		ProvidePasswordCallback = other.ProvidePasswordCallback;
		RemoteCertificateValidationCallback = other.RemoteCertificateValidationCallback;
	}

	private void VerifyNotDisposed()
	{
		if (m_isDisposed)
			throw new ObjectDisposedException(GetType().Name);
	}

	private async Task CloseAsync(bool changeState, IOBehavior ioBehavior)
	{
		// check fast path
		if (m_activeReader is null &&
			CurrentTransaction is null &&
			m_enlistedTransaction is null &&
			(m_connectionSettings?.Pooling is true))
		{
			m_cachedProcedures = null;
			if (m_session is not null)
			{
				await m_session.ReturnToPoolAsync(ioBehavior, this).ConfigureAwait(false);
				m_session = null;
			}
			if (changeState)
				SetState(ConnectionState.Closed);

			return;
		}

		await DoCloseAsync(changeState, ioBehavior).ConfigureAwait(false);
	}

	private async Task DoCloseAsync(bool changeState, IOBehavior ioBehavior)
	{
		// If participating in a distributed transaction, keep the connection open so we can commit or rollback.
		// This handles the common pattern of disposing a connection before disposing a TransactionScope (e.g., nested using blocks)
		if (m_enlistedTransaction is not null)
		{
			// make sure all DB work is done
			if (m_activeReader is not null)
				await m_activeReader.DisposeAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
			m_activeReader = null;

			// This connection is being closed, so create a new MySqlConnection that will own the ServerSession
			// (which remains open). This ensures the ServerSession always has a valid OwningConnection (even
			// if 'this' is GCed.
			var connection = new MySqlConnection
			{
				m_connectionString = m_connectionString,
				m_connectionSettings = m_connectionSettings,
				m_connectionState = m_connectionState,
				m_hasBeenOpened = true,
			};
			connection.TakeSessionFrom(this);

			// put the new, idle, connection into the list of sessions for this transaction (replacing this MySqlConnection)
			lock (s_lock)
			{
				foreach (var enlistedTransaction in s_transactionConnections[connection.m_enlistedTransaction!.Transaction])
				{
					if (enlistedTransaction.Connection == this)
					{
						enlistedTransaction.Connection = connection;
						enlistedTransaction.IsIdle = true;
						break;
					}
				}
			}

			if (changeState)
				SetState(ConnectionState.Closed);
			return;
		}

		m_cachedProcedures = null;

		try
		{
			if (m_activeReader is not null || CurrentTransaction is not null)
				await CloseDatabaseAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
		}
		finally
		{
			if (m_session is not null)
			{
				if (GetInitializedConnectionSettings().Pooling)
				{
					await m_session.ReturnToPoolAsync(ioBehavior, this).ConfigureAwait(false);
				}
				else
				{
					await m_session.DisposeAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
					m_session.OwningConnection = null;
				}
				m_session = null;
			}

			if (changeState)
				SetState(ConnectionState.Closed);
		}
	}

	private async ValueTask CloseDatabaseAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		if (m_activeReader is not null)
			await m_activeReader.DisposeAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
		if (CurrentTransaction is not null && m_session!.IsConnected)
		{
			await CurrentTransaction.DisposeAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
			CurrentTransaction = null;
		}
	}

	private ConnectionSettings GetConnectionSettings() =>
		m_connectionSettings ??= new(new MySqlConnectionStringBuilder(m_connectionString));

	// This method may be called when it's known that the connection settings have been initialized.
	private ConnectionSettings GetInitializedConnectionSettings() => m_connectionSettings!;

	private static readonly StateChangeEventArgs s_stateChangeClosedConnecting = new(ConnectionState.Closed, ConnectionState.Connecting);
	private static readonly StateChangeEventArgs s_stateChangeConnectingOpen = new(ConnectionState.Connecting, ConnectionState.Open);
	private static readonly StateChangeEventArgs s_stateChangeOpenClosed = new(ConnectionState.Open, ConnectionState.Closed);
	private static readonly object s_lock = new();
	private static readonly Dictionary<System.Transactions.Transaction, List<EnlistedTransactionBase>> s_transactionConnections = new();

	private readonly MySqlDataSource? m_dataSource;
	private readonly ILogger m_logger;
	private string m_connectionString;
	private ConnectionSettings? m_connectionSettings;
	private ServerSession? m_session;
	private ConnectionState m_connectionState;
	private bool m_hasBeenOpened;
	private bool m_isDisposed;
	private Dictionary<string, CachedProcedure?>? m_cachedProcedures;
	private SchemaProvider? m_schemaProvider;
	private MySqlDataReader? m_activeReader;
	private EnlistedTransactionBase? m_enlistedTransaction;
}
