using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector.Core;
using MySqlConnector.Logging;
using MySqlConnector.Protocol.Payloads;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySql.Data.MySqlClient
{
	public sealed class MySqlConnection : DbConnection
#if !NETSTANDARD1_3
		, ICloneable
#endif
	{
		public MySqlConnection()
			: this(default)
		{
		}

		public MySqlConnection(string? connectionString)
		{
			GC.SuppressFinalize(this);
			m_connectionString = connectionString ?? "";
		}

		public new MySqlTransaction BeginTransaction() => (MySqlTransaction) base.BeginTransaction();
		public new MySqlTransaction BeginTransaction(IsolationLevel isolationLevel) => (MySqlTransaction) base.BeginTransaction(isolationLevel);
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => BeginDbTransactionAsync(isolationLevel, IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

#if !NETSTANDARD2_1 && !NETCOREAPP3_0
		public ValueTask<MySqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default) => BeginDbTransactionAsync(IsolationLevel.Unspecified, AsyncIOBehavior, cancellationToken);
		public ValueTask<MySqlTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default) => BeginDbTransactionAsync(isolationLevel, AsyncIOBehavior, cancellationToken);
#else
		public new ValueTask<MySqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default) => BeginDbTransactionAsync(IsolationLevel.Unspecified, AsyncIOBehavior, cancellationToken);
		public new ValueTask<MySqlTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default) => BeginDbTransactionAsync(isolationLevel, AsyncIOBehavior, cancellationToken);

		protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken) =>
			await BeginDbTransactionAsync(isolationLevel, AsyncIOBehavior, cancellationToken).ConfigureAwait(false);
#endif

		private async ValueTask<MySqlTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (State != ConnectionState.Open)
				throw new InvalidOperationException("Connection is not open.");
			if (CurrentTransaction is object)
				throw new InvalidOperationException("Transactions may not be nested.");
#if !NETSTANDARD1_3
			if (m_enlistedTransaction is object)
				throw new InvalidOperationException("Cannot begin a transaction when already enlisted in a transaction.");
#endif

			string isolationLevelValue = isolationLevel switch
			{
				IsolationLevel.ReadUncommitted => "read uncommitted",
				IsolationLevel.ReadCommitted => "read committed",
				IsolationLevel.RepeatableRead => "repeatable read",
				IsolationLevel.Serializable => "serializable",

				// "In terms of the SQL:1992 transaction isolation levels, the default InnoDB level is REPEATABLE READ." - http://dev.mysql.com/doc/refman/5.7/en/innodb-transaction-model.html
				IsolationLevel.Unspecified => "repeatable read",

				_ => throw new NotSupportedException("IsolationLevel.{0} is not supported.".FormatInvariant(isolationLevel))
			};

			using (var cmd = new MySqlCommand("set transaction isolation level " + isolationLevelValue + "; start transaction;", this))
				await cmd.ExecuteNonQueryAsync(ioBehavior, cancellationToken).ConfigureAwait(false);

			var transaction = new MySqlTransaction(this, isolationLevel);
			CurrentTransaction = transaction;
			return transaction;
		}

#if !NETSTANDARD1_3
		public override void EnlistTransaction(System.Transactions.Transaction? transaction)
		{
			if (State != ConnectionState.Open)
				throw new InvalidOperationException("Connection is not open.");

			// ignore reenlistment of same connection in same transaction
			if (m_enlistedTransaction?.Transaction.Equals(transaction) ?? false)
				return;

			if (m_enlistedTransaction is object)
				throw new MySqlException("Already enlisted in a Transaction.");
			if (CurrentTransaction is object)
				throw new InvalidOperationException("Can't enlist in a Transaction when there is an active MySqlTransaction.");

			if (transaction is object)
			{
				var existingConnection = FindExistingEnlistedSession(transaction);
				if (existingConnection is object)
				{
					// can reuse the existing connection
					CloseAsync(changeState: false, IOBehavior.Synchronous).GetAwaiter().GetResult();
					TakeSessionFrom(existingConnection);
					return;
				}
				else
				{
					m_enlistedTransaction = GetInitializedConnectionSettings().UseXaTransactions ?
						(EnlistedTransactionBase) new XaEnlistedTransaction(transaction, this) :
						new StandardEnlistedTransaction(transaction, this);
					m_enlistedTransaction.Start();

					lock (s_lock)
					{
						if (!s_transactionConnections.TryGetValue(transaction, out var enlistedTransactions))
							s_transactionConnections[transaction] = enlistedTransactions = new List<EnlistedTransactionBase>();
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
			if (m_session is object)
				throw new InvalidOperationException("This connection must not have a session");
			if (other.m_session is null)
				throw new InvalidOperationException("Other connection must have a session");
			if (m_enlistedTransaction is object)
				throw new InvalidOperationException("This connection must not have an enlisted transaction");
			if (other.m_enlistedTransaction is null)
				throw new InvalidOperationException("Other connection must have an enlisted transaction");
			if (m_activeReader is object)
				throw new InvalidOperationException("This connection must not have an active reader");
			if (other.m_activeReader is object)
				throw new InvalidOperationException("Other connection must not have an active reader");
#endif

			m_session = other.m_session;
			m_session!.OwningConnection = new WeakReference<MySqlConnection>(this);
			other.m_session = null;

			m_cachedProcedures = other.m_cachedProcedures;
			other.m_cachedProcedures = null;

			m_enlistedTransaction = other.m_enlistedTransaction;
			other.m_enlistedTransaction = null;
		}

		EnlistedTransactionBase? m_enlistedTransaction;
#endif

		public override void Close() => CloseAsync(changeState: true, IOBehavior.Synchronous).GetAwaiter().GetResult();
#if !NETSTANDARD2_1 && !NETCOREAPP3_0
		public Task CloseAsync() => CloseAsync(changeState: true, SimpleAsyncIOBehavior);
#else
		public override Task CloseAsync() => CloseAsync(changeState: true, SimpleAsyncIOBehavior);
#endif
		internal Task CloseAsync(IOBehavior ioBehavior) => CloseAsync(changeState: true, ioBehavior);

		public override void ChangeDatabase(string databaseName) => ChangeDatabaseAsync(IOBehavior.Synchronous, databaseName, CancellationToken.None).GetAwaiter().GetResult();
#if !NETSTANDARD2_1 && !NETCOREAPP3_0
		public Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default) => ChangeDatabaseAsync(AsyncIOBehavior, databaseName, cancellationToken);
#else
		public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default) => ChangeDatabaseAsync(AsyncIOBehavior, databaseName, cancellationToken);
#endif

		private async Task ChangeDatabaseAsync(IOBehavior ioBehavior, string databaseName, CancellationToken cancellationToken)
		{
			if (string.IsNullOrWhiteSpace(databaseName))
				throw new ArgumentException("Database name is not valid.", nameof(databaseName));
			if (State != ConnectionState.Open)
				throw new InvalidOperationException("Connection is not open.");

			await CloseDatabaseAsync(ioBehavior, cancellationToken).ConfigureAwait(false);

			using (var initDatabasePayload = InitDatabasePayload.Create(databaseName))
				await m_session!.SendAsync(initDatabasePayload, ioBehavior, cancellationToken).ConfigureAwait(false);
			var payload = await m_session.ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
			OkPayload.Create(payload.Span, m_session.SupportsDeprecateEof, m_session.SupportsSessionTrack);
			m_session.DatabaseOverride = databaseName;
		}

		public new MySqlCommand CreateCommand() => (MySqlCommand) base.CreateCommand();

		public bool Ping() => PingAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();
		public Task<bool> PingAsync(CancellationToken cancellationToken = default) => PingAsync(SimpleAsyncIOBehavior, cancellationToken).AsTask();

		private async ValueTask<bool> PingAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (m_session is null)
				return false;
			try
			{
				if (await m_session.TryPingAsync(ioBehavior, cancellationToken).ConfigureAwait(false))
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

		private async Task OpenAsync(IOBehavior? ioBehavior, CancellationToken cancellationToken)
		{
			VerifyNotDisposed();
			if (State != ConnectionState.Closed)
				throw new InvalidOperationException("Cannot Open when State is {0}.".FormatInvariant(State));

			SetState(ConnectionState.Connecting);

			var pool = ConnectionPool.GetPool(m_connectionString);
			m_connectionSettings ??= pool?.ConnectionSettings ?? new ConnectionSettings(new MySqlConnectionStringBuilder(m_connectionString));

#if !NETSTANDARD1_3
			// check if there is an open session (in the current transaction) that can be adopted
			if (m_connectionSettings.AutoEnlist && System.Transactions.Transaction.Current is object)
			{
				var existingConnection = FindExistingEnlistedSession(System.Transactions.Transaction.Current);
				if (existingConnection is object)
				{
					TakeSessionFrom(existingConnection);
					m_hasBeenOpened = true;
					SetState(ConnectionState.Open);
					return;
				}
			}
#endif

			try
			{
				m_session = await CreateSessionAsync(pool, ioBehavior, cancellationToken).ConfigureAwait(false);

				m_hasBeenOpened = true;
				SetState(ConnectionState.Open);
			}
			catch (MySqlException)
			{
				SetState(ConnectionState.Closed);
				throw;
			}
			catch (SocketException ex)
			{
				SetState(ConnectionState.Closed);
				throw new MySqlException(MySqlErrorCode.UnableToConnectToHost, "Unable to connect to any of the specified MySQL hosts.", ex);
			}

#if !NETSTANDARD1_3
			if (m_connectionSettings.AutoEnlist && System.Transactions.Transaction.Current is object)
				EnlistTransaction(System.Transactions.Transaction.Current);
#endif
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

		public int ServerThread => Session.ConnectionId;

		public static void ClearPool(MySqlConnection connection) => ClearPoolAsync(connection, IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();
		public static Task ClearPoolAsync(MySqlConnection connection, CancellationToken cancellationToken = default) => ClearPoolAsync(connection, connection.AsyncIOBehavior, cancellationToken);
		public static void ClearAllPools() => ConnectionPool.ClearPoolsAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();
		public static Task ClearAllPoolsAsync(CancellationToken cancellationToken = default) => ConnectionPool.ClearPoolsAsync(IOBehavior.Asynchronous, cancellationToken);

		private static async Task ClearPoolAsync(MySqlConnection connection, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			var pool = ConnectionPool.GetPool(connection.m_connectionString);
			if (pool is object)
				await pool.ClearAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
		}

		protected override DbCommand CreateDbCommand() => new MySqlCommand(this, null);

#if !NETSTANDARD1_3
		protected override DbProviderFactory DbProviderFactory => MySqlClientFactory.Instance;

		/// <inheritdoc cref="DbConnection.GetSchema()"/>
		public override DataTable GetSchema() => GetSchemaProvider().GetSchema();

		/// <inheritdoc cref="DbConnection.GetSchema(string)"/>
		public override DataTable GetSchema(string collectionName) => GetSchemaProvider().GetSchema(collectionName);

		/// <inheritdoc cref="DbConnection.GetSchema(string)"/>
		public override DataTable GetSchema(string collectionName, string?[]? restrictions) => GetSchemaProvider().GetSchema(collectionName);

		private SchemaProvider GetSchemaProvider()
		{
			if (m_schemaProvider is null)
				m_schemaProvider = new SchemaProvider(this);
			return m_schemaProvider;
		}

		SchemaProvider? m_schemaProvider;
#endif

 		/// <summary>
        /// Gets the time (in seconds) to wait while trying to establish a connection
        /// before terminating the attempt and generating an error. The default value is 15 seconds.
        /// </summary>
		public override int ConnectionTimeout => GetConnectionSettings().ConnectionTimeout;

		public event MySqlInfoMessageEventHandler? InfoMessage;

		public MySqlBatch CreateBatch() => CreateDbBatch();
		private MySqlBatch CreateDbBatch() => new MySqlBatch(this);

		public MySqlBatchCommand CreateBatchCommand() => CreateDbBatchCommand();
		private MySqlBatchCommand CreateDbBatchCommand() => new MySqlBatchCommand();
		public bool CanCreateBatch => true;

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

#if !NETSTANDARD2_1 && !NETCOREAPP3_0
		public async Task DisposeAsync()
#else
		public override async ValueTask DisposeAsync()
#endif
		{
			try
			{
				await CloseAsync(changeState: true, SimpleAsyncIOBehavior).ConfigureAwait(false);
			}
			finally
			{
				m_isDisposed = true;
			}
		}

		public MySqlConnection Clone() => new MySqlConnection(m_connectionString);

#if !NETSTANDARD1_3
		object ICloneable.Clone() => Clone();
#endif

		internal ServerSession Session
		{
			get
			{
				VerifyNotDisposed();
				if (m_session is null || State != ConnectionState.Open)
					throw new InvalidOperationException("Connection must be Open; current state is {0}".FormatInvariant(State));
				return m_session;
			}
		}

		internal void SetSessionFailed(Exception exception) => m_session!.SetFailed(exception);

		internal void Cancel(ICancellableCommand command)
		{
			var session = Session;
			if (!session.TryStartCancel(command))
				return;

			try
			{
				// open a dedicated connection to the server to kill the active query
				var csb = new MySqlConnectionStringBuilder(m_connectionString);
				csb.Pooling = false;
				if (m_session!.IPAddress is object)
					csb.Server = m_session.IPAddress.ToString();
				csb.ConnectionTimeout = 3u;

				using var connection = new MySqlConnection(csb.ConnectionString);
				connection.Open();
				using var killCommand = new MySqlCommand("KILL QUERY {0}".FormatInvariant(command.Connection!.ServerThread), connection);
				session.DoCancel(command, killCommand);
			}
			catch (MySqlException ex)
			{
				// cancelling the query failed; setting the state back to 'Querying' will allow another call to 'Cancel' to try again
				Log.Warn(ex, "Session{0} cancelling command {1} failed", m_session!.Id, command.CommandId);
				session.AbortCancel(command);
			}
		}

		internal async Task<CachedProcedure?> GetCachedProcedure(string name, bool revalidateMissing, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (Log.IsDebugEnabled())
				Log.Debug("Session{0} getting cached procedure Name={1}", m_session!.Id, name);
			if (State != ConnectionState.Open)
				throw new InvalidOperationException("Connection is not open.");

			var cachedProcedures = m_session!.Pool?.GetProcedureCache() ?? m_cachedProcedures;
			if (cachedProcedures is null)
			{
				Log.Warn("Session{0} pool Pool{1} doesn't have a shared procedure cache; procedure will only be cached on this connection", m_session.Id, m_session.Pool?.Id);
				cachedProcedures = m_cachedProcedures = new Dictionary<string, CachedProcedure?>();
			}

			var normalized = NormalizedSchema.MustNormalize(name, Database);
			if (string.IsNullOrEmpty(normalized.Schema))
			{
				Log.Warn("Session{0} couldn't normalize Database={1} Name={2}; not caching procedure", m_session.Id, Database, name);
				return null;
			}

			CachedProcedure? cachedProcedure;
			bool foundProcedure;
			lock (cachedProcedures)
				foundProcedure = cachedProcedures.TryGetValue(normalized.FullyQualified, out cachedProcedure);
			if (!foundProcedure || (cachedProcedure is null && revalidateMissing))
			{
				cachedProcedure = await CachedProcedure.FillAsync(ioBehavior, this, normalized.Schema!, normalized.Component!, cancellationToken).ConfigureAwait(false);
				if (Log.IsWarnEnabled())
				{
					if (cachedProcedure is null)
						Log.Warn("Session{0} failed to cache procedure Schema={1} Component={2}", m_session.Id, normalized.Schema, normalized.Component);
					else
						Log.Info("Session{0} caching procedure Schema={1} Component={2}", m_session.Id, normalized.Schema, normalized.Component);
				}
				int count;
				lock (cachedProcedures)
				{
					cachedProcedures[normalized.FullyQualified] = cachedProcedure;
					count = cachedProcedures.Count;
				}
				if (Log.IsInfoEnabled())
					Log.Info("Session{0} procedure cache Count={1}", m_session.Id, count);
			}

			if (Log.IsWarnEnabled())
			{
				if (cachedProcedure is null)
					Log.Warn("Session{0} did not find cached procedure Schema={1} Component={2}", m_session.Id, normalized.Schema, normalized.Component);
				else
					Log.Debug("Session{0} returning cached procedure Schema={1} Component={2}", m_session.Id, normalized.Schema, normalized.Component);
			}
			return cachedProcedure;
		}

		internal MySqlTransaction? CurrentTransaction { get; set; }
		internal bool AllowLoadLocalInfile => GetInitializedConnectionSettings().AllowLoadLocalInfile;
		internal bool AllowUserVariables => GetInitializedConnectionSettings().AllowUserVariables;
		internal bool AllowZeroDateTime => GetInitializedConnectionSettings().AllowZeroDateTime;
		internal bool ConvertZeroDateTime => GetInitializedConnectionSettings().ConvertZeroDateTime;
		internal DateTimeKind DateTimeKind => GetInitializedConnectionSettings().DateTimeKind;
		internal int DefaultCommandTimeout => GetConnectionSettings().DefaultCommandTimeout;
		internal MySqlGuidFormat GuidFormat => GetInitializedConnectionSettings().GuidFormat;
#if NETSTANDARD1_3
		internal bool IgnoreCommandTransaction => GetInitializedConnectionSettings().IgnoreCommandTransaction;
#else
		internal bool IgnoreCommandTransaction => GetInitializedConnectionSettings().IgnoreCommandTransaction || m_enlistedTransaction is StandardEnlistedTransaction;
#endif
		internal bool IgnorePrepare => GetInitializedConnectionSettings().IgnorePrepare;
		internal bool NoBackslashEscapes => GetInitializedConnectionSettings().NoBackslashEscapes;
		internal bool TreatTinyAsBoolean => GetInitializedConnectionSettings().TreatTinyAsBoolean;
		internal IOBehavior AsyncIOBehavior => GetConnectionSettings().ForceSynchronous ? IOBehavior.Synchronous : IOBehavior.Asynchronous;

		// Defaults to IOBehavior.Synchronous if the connection hasn't been opened yet; only use if it's a no-op for a closed connection.
		internal IOBehavior SimpleAsyncIOBehavior => (m_connectionSettings?.ForceSynchronous ?? false) ? IOBehavior.Synchronous : IOBehavior.Asynchronous;

		internal MySqlSslMode SslMode => GetInitializedConnectionSettings().SslMode;

		internal bool HasActiveReader => m_activeReader is object;

		internal void SetActiveReader(MySqlDataReader dataReader)
		{
			if (dataReader is null)
				throw new ArgumentNullException(nameof(dataReader));
			if (m_activeReader is object)
				throw new InvalidOperationException("Can't replace active reader.");
			m_activeReader = dataReader;
		}

		internal void FinishQuerying(bool hasWarnings)
		{
			m_session!.FinishQuerying();
			m_activeReader = null;

			if (hasWarnings && InfoMessage is object)
			{
				var errors = new List<MySqlError>();
				using (var command = new MySqlCommand("SHOW WARNINGS;", this))
				using (var reader = command.ExecuteReader())
				{
					while (reader.Read())
						errors.Add(new MySqlError(reader.GetString(0), reader.GetInt32(1), reader.GetString(2)));
				}

				InfoMessage(this, new MySqlInfoMessageEventArgs(errors.ToArray()));
			}
		}

		private async ValueTask<ServerSession> CreateSessionAsync(ConnectionPool? pool, IOBehavior? ioBehavior, CancellationToken cancellationToken)
		{
			var connectionSettings = GetInitializedConnectionSettings();
			var actualIOBehavior = ioBehavior ?? (connectionSettings.ForceSynchronous ? IOBehavior.Synchronous : IOBehavior.Asynchronous);

			CancellationTokenSource? timeoutSource = null;
			CancellationTokenSource? linkedSource = null;
			try
			{
				// the cancellation token for connection is controlled by 'cancellationToken' (if it can be cancelled), ConnectionTimeout
				// (from the connection string, if non-zero), or a combination of both
				if (connectionSettings.ConnectionTimeout != 0)
					timeoutSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(connectionSettings.ConnectionTimeoutMilliseconds));
				if (cancellationToken.CanBeCanceled && timeoutSource is object)
					linkedSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);
				var connectToken = linkedSource?.Token ?? timeoutSource?.Token ?? cancellationToken;

				// get existing session from the pool if possible
				if (pool is object)
				{
					// this returns an open session
					return await pool.GetSessionAsync(this, actualIOBehavior, connectToken).ConfigureAwait(false);
				}
				else
				{
					// only "fail over" and "random" load balancers supported without connection pooling
					var loadBalancer = connectionSettings.LoadBalance == MySqlLoadBalance.Random && connectionSettings.HostNames!.Count > 1 ?
						RandomLoadBalancer.Instance : FailOverLoadBalancer.Instance;

					var session = new ServerSession();
					session.OwningConnection = new WeakReference<MySqlConnection>(this);
					Log.Info("Created new non-pooled Session{0}", session.Id);
					await session.ConnectAsync(connectionSettings, loadBalancer, actualIOBehavior, connectToken).ConfigureAwait(false);
					return session;
				}
			}
			catch (OperationCanceledException ex) when (timeoutSource?.IsCancellationRequested ?? false)
			{
				throw new MySqlException(MySqlErrorCode.UnableToConnectToHost, "Connect Timeout expired.", ex);
			}
			finally
			{
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
					new StateChangeEventArgs(previousState, newState);
				OnStateChange(eventArgs);
			}
		}

		private void VerifyNotDisposed()
		{
			if (m_isDisposed)
				throw new ObjectDisposedException(GetType().Name);
		}

		private Task CloseAsync(bool changeState, IOBehavior ioBehavior)
		{
			if (m_connectionState == ConnectionState.Closed)
				return Utility.CompletedTask;

			// check fast path
			if (m_activeReader is null &&
				CurrentTransaction is null &&
#if !NETSTANDARD1_3
				m_enlistedTransaction is null &&
#endif
				(m_connectionSettings?.Pooling ?? false))
			{
				m_cachedProcedures = null;
				if (m_session is object)
				{
					m_session.ReturnToPool();
					m_session = null;
				}
				if (changeState)
					SetState(ConnectionState.Closed);

				return Utility.CompletedTask;
			}

			return DoCloseAsync(changeState, ioBehavior);
		}

		private async Task DoCloseAsync(bool changeState, IOBehavior ioBehavior)
		{
#if !NETSTANDARD1_3
			// If participating in a distributed transaction, keep the connection open so we can commit or rollback.
			// This handles the common pattern of disposing a connection before disposing a TransactionScope (e.g., nested using blocks)
			if (m_enlistedTransaction is object)
			{
				// make sure all DB work is done
				if (m_activeReader is object)
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
#endif

			if (m_connectionState != ConnectionState.Closed)
			{
				try
				{
					await CloseDatabaseAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
				}
				finally
				{
					if (m_session is object)
					{
						if (GetInitializedConnectionSettings().Pooling)
						{
							m_session.ReturnToPool();
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
		}

		private Task CloseDatabaseAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			m_cachedProcedures = null;
			if (m_activeReader is null && CurrentTransaction is null)
				return Utility.CompletedTask;
			return DoCloseDatabaseAsync(ioBehavior, cancellationToken);
		}

		private async Task DoCloseDatabaseAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (m_activeReader is object)
				await m_activeReader.DisposeAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
			if (CurrentTransaction is object && m_session!.IsConnected)
			{
				await CurrentTransaction.DisposeAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				CurrentTransaction = null;
			}
		}

		private ConnectionSettings GetConnectionSettings() =>
			m_connectionSettings ??= new ConnectionSettings(new MySqlConnectionStringBuilder(m_connectionString));

		// This method may be called when it's known that the connection settings have been initialized.
		private ConnectionSettings GetInitializedConnectionSettings() => m_connectionSettings!;

		static readonly IMySqlConnectorLogger Log = MySqlConnectorLogManager.CreateLogger(nameof(MySqlConnection));
		static readonly StateChangeEventArgs s_stateChangeClosedConnecting = new StateChangeEventArgs(ConnectionState.Closed, ConnectionState.Connecting);
		static readonly StateChangeEventArgs s_stateChangeConnectingOpen = new StateChangeEventArgs(ConnectionState.Connecting, ConnectionState.Open);
		static readonly StateChangeEventArgs s_stateChangeOpenClosed = new StateChangeEventArgs(ConnectionState.Open, ConnectionState.Closed);
#if !NETSTANDARD1_3
		static readonly object s_lock = new object();
		static readonly Dictionary<System.Transactions.Transaction, List<EnlistedTransactionBase>> s_transactionConnections = new Dictionary<System.Transactions.Transaction, List<EnlistedTransactionBase>>();
#endif

		string m_connectionString;
		ConnectionSettings? m_connectionSettings;
		ServerSession? m_session;
		ConnectionState m_connectionState;
		bool m_hasBeenOpened;
		bool m_isDisposed;
		Dictionary<string, CachedProcedure?>? m_cachedProcedures;
		MySqlDataReader? m_activeReader;
	}
}
