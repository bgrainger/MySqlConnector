using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net;
using System.Security.Authentication;
using Microsoft.Extensions.Logging;
using MySqlConnector.Logging;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Core;

internal sealed class ConnectionPool : IDisposable
{
	public int Id { get; }

	public string? Name { get; }

	public ConnectionSettings ConnectionSettings { get; }

	public SslProtocols SslProtocols { get; set; }

	public void AddPendingRequestCount(int delta) => s_pendingRequestsCounter.Add(delta, PoolNameTagList);

	public async ValueTask<ServerSession> GetSessionAsync(MySqlConnection connection, int startTickCount, Activity? activity, IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		// if all sessions are used, see if any have been leaked and can be recovered
		// check at most once per second (although this isn't enforced via a mutex so multiple threads might block
		// on the lock in RecoverLeakedSessions in high-concurrency situations
		if (IsEmpty && unchecked(((uint) Environment.TickCount) - m_lastRecoveryTime) >= 1000u)
		{
			Log.ScanningForLeakedSessions(m_logger, Id);
			await RecoverLeakedSessionsAsync(ioBehavior).ConfigureAwait(false);
		}

		if (ConnectionSettings.MinimumPoolSize > 0)
			await CreateMinimumPooledSessions(connection, ioBehavior, cancellationToken).ConfigureAwait(false);

		// wait for an open slot (until the cancellationToken is cancelled, which is typically due to timeout)
		Log.WaitingForAvailableSession(m_logger, Id);
		if (ioBehavior == IOBehavior.Asynchronous)
			await m_sessionSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
		else
			m_sessionSemaphore.Wait(cancellationToken);

		ServerSession? session = null;
		try
		{
			// check for a waiting session
			lock (m_sessions)
			{
				if (m_sessions.Count > 0)
				{
					// NOTE: s_connectionsUsageCounter updated outside lock below
					session = m_sessions.First!.Value;
					m_sessions.RemoveFirst();
				}
			}
			if (session is not null)
			{
				s_connectionsUsageCounter.Add(-1, IdleStateTagList);
				Log.FoundExistingSession(m_logger, Id);
				bool reuseSession;

				if (session.PoolGeneration != m_generation)
				{
					Log.DiscardingSessionDueToWrongGeneration(m_logger, Id);
					reuseSession = false;
				}
				else
				{
					if (ConnectionSettings.ConnectionReset || session.DatabaseOverride is not null)
						reuseSession = await session.TryResetConnectionAsync(ConnectionSettings, connection, ioBehavior, cancellationToken).ConfigureAwait(false);
					else
						reuseSession = true;
				}

				if (!reuseSession)
				{
					// session is either old or cannot communicate with the server
					Log.SessionIsUnusable(m_logger, Id, session.Id);
					AdjustHostConnectionCount(session, -1);
					await session.DisposeAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				}
				else
				{
					// pooled session is ready to be used; return it
					session.OwningConnection = new(connection);
					int leasedSessionsCountPooled;
					lock (m_leasedSessions)
					{
						m_leasedSessions.Add(session.Id, session);
						leasedSessionsCountPooled = m_leasedSessions.Count;
					}
					s_connectionsUsageCounter.Add(1, UsedStateTagList);
					ActivitySourceHelper.CopyTags(session.ActivityTags, activity);
					Log.ReturningPooledSession(m_logger, Id, session.Id, leasedSessionsCountPooled);

					session.LastLeasedTicks = unchecked((uint) Environment.TickCount);
					s_waitTimeHistory.Record(unchecked(session.LastLeasedTicks - (uint) startTickCount), PoolNameTagList);
					return session;
				}
			}

			// create a new session
			session = await ConnectSessionAsync(connection, s_createdNewSession, startTickCount, activity, ioBehavior, cancellationToken).ConfigureAwait(false);
			AdjustHostConnectionCount(session, 1);
			session.OwningConnection = new(connection);
			int leasedSessionsCountNew;
			lock (m_leasedSessions)
			{
				m_leasedSessions.Add(session.Id, session);
				leasedSessionsCountNew = m_leasedSessions.Count;
			}
			s_connectionsUsageCounter.Add(1, UsedStateTagList);
			Log.ReturningNewSession(m_logger, Id, session.Id, leasedSessionsCountNew);

			session.LastLeasedTicks = unchecked((uint) Environment.TickCount);
			s_createTimeHistory.Record(unchecked(session.LastLeasedTicks - (uint) startTickCount), PoolNameTagList);
			return session;
		}
		catch (Exception ex)
		{
			if (session is not null)
			{
				try
				{
					Log.DisposingCreatedSessionDueToException(m_logger, ex, Id, session.Id, ex.Message);
					AdjustHostConnectionCount(session, -1);
					await session.DisposeAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
				}
				catch (Exception unexpectedException)
				{
					Log.UnexpectedErrorInGetSessionAsync(m_logger, unexpectedException, Id, unexpectedException.Message);
				}
			}

			m_sessionSemaphore.Release();
			throw;
		}
	}

	/// <summary>
	/// Returns <c>true</c> if the connection pool is empty, i.e., all connections are in use. Note that in a highly-multithreaded
	/// environment, the value of this property may be stale by the time it's returned.
	/// </summary>
	internal bool IsEmpty => m_sessionSemaphore.CurrentCount == 0;

	// Returns zero for healthy, non-zero otherwise.
	private int GetSessionHealth(ServerSession session)
	{
		if (!session.IsConnected)
			return 1;
		if (session.PoolGeneration != m_generation)
			return 2;
		if (ConnectionSettings.ConnectionLifeTime > 0
			&& unchecked((uint) Environment.TickCount) - session.CreatedTicks >= ConnectionSettings.ConnectionLifeTime)
			return 3;

		return 0;
	}

	public async ValueTask ReturnAsync(IOBehavior ioBehavior, ServerSession session)
	{
		Log.ReceivingSessionBack(m_logger, Id, session.Id);

		try
		{
			lock (m_leasedSessions)
				m_leasedSessions.Remove(session.Id);
			s_connectionsUsageCounter.Add(-1, UsedStateTagList);
			session.OwningConnection = null;
			var sessionHealth = GetSessionHealth(session);
			if (sessionHealth == 0)
			{
				lock (m_sessions)
					m_sessions.AddFirst(session);
				s_connectionsUsageCounter.Add(1, IdleStateTagList);
			}
			else
			{
				if (sessionHealth == 1)
					Log.ReceivedInvalidSession(m_logger, Id, session.Id);
				else
					Log.ReceivedExpiredSession(m_logger, Id, session.Id);
				AdjustHostConnectionCount(session, -1);
				await session.DisposeAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
			}
		}
		finally
		{
			m_sessionSemaphore.Release();
		}
	}

	public async Task ClearAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		// increment the generation of the connection pool
		Log.ClearingConnectionPool(m_logger, Id);
		Interlocked.Increment(ref m_generation);
		m_procedureCache = null;
		await RecoverLeakedSessionsAsync(ioBehavior).ConfigureAwait(false);
		await CleanPoolAsync(ioBehavior, session => session.PoolGeneration != m_generation, false, cancellationToken).ConfigureAwait(false);
	}

	public async Task ReapAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		Log.ReapingConnectionPool(m_logger, Id);
		await RecoverLeakedSessionsAsync(ioBehavior).ConfigureAwait(false);
		await CleanPoolAsync(ioBehavior, session => (unchecked((uint) Environment.TickCount) - session.LastReturnedTicks) / 1000 >= ConnectionSettings.ConnectionIdleTimeout, true, cancellationToken).ConfigureAwait(false);
	}

	/// <summary>
	/// Returns the stored procedure cache for this <see cref="ConnectionPool"/>, lazily creating it on demand.
	/// This method may return a different object after <see cref="ClearAsync"/> has been called. The returned
	/// object is shared between multiple threads and is only safe to use after taking a <c>lock</c> on the
	/// object itself.
	/// </summary>
	public Dictionary<string, CachedProcedure?> GetProcedureCache()
	{
		var procedureCache = m_procedureCache;
		if (procedureCache is null)
		{
			var newProcedureCache = new Dictionary<string, CachedProcedure?>();
			procedureCache = Interlocked.CompareExchange(ref m_procedureCache, newProcedureCache, null) ?? newProcedureCache;
		}
		return procedureCache;
	}

	public void Dispose()
	{
		Log.DisposingConnectionPool(m_logger, Id);
#if NET6_0_OR_GREATER
		m_dnsCheckTimer?.Dispose();
		m_dnsCheckTimer = null;
		m_reaperTimer?.Dispose();
		m_reaperTimer = null;
#else
		if (m_dnsCheckTimer is not null)
		{
			using var dnsCheckWaitHandle = new ManualResetEvent(false);
			m_dnsCheckTimer.Dispose(dnsCheckWaitHandle);
			dnsCheckWaitHandle.WaitOne();
		}
		if (m_reaperTimer is not null)
		{
			using var reaperWaitHandle = new ManualResetEvent(false);
			m_reaperTimer.Dispose(reaperWaitHandle);
			reaperWaitHandle.WaitOne();
		}
#endif

		s_minIdleConnectionsCounter.Add(-ConnectionSettings.MinimumPoolSize, PoolNameTagList);
		s_maxIdleConnectionsCounter.Add(-ConnectionSettings.MaximumPoolSize, PoolNameTagList);
		s_maxConnectionsCounter.Add(-ConnectionSettings.MaximumPoolSize, PoolNameTagList);
	}

	/// <summary>
	/// Examines all the <see cref="ServerSession"/> objects in <see cref="m_leasedSessions"/> to determine if any
	/// have an owning <see cref="MySqlConnection"/> that has been garbage-collected. If so, assumes that the connection
	/// was not properly disposed and returns the session to the pool.
	/// </summary>
	private async Task RecoverLeakedSessionsAsync(IOBehavior ioBehavior)
	{
		var recoveredSessions = new List<(ServerSession Session, MySqlConnection Connection)>();
		lock (m_leasedSessions)
		{
			m_lastRecoveryTime = unchecked((uint) Environment.TickCount);
			foreach (var session in m_leasedSessions.Values)
			{
				if (!session.OwningConnection!.TryGetTarget(out var _))
				{
					// create a dummy MySqlConnection so that any thread running RecoverLeakedSessionsAsync doesn't process this one
					var connection = new MySqlConnection();
					session.OwningConnection = new(connection);
					recoveredSessions.Add((session, connection));
				}
			}
		}
		if (recoveredSessions.Count == 0)
			Log.RecoveredNoSessions(m_logger, Id);
		else
			Log.RecoveredSessionCount(m_logger, Id, recoveredSessions.Count);

		foreach (var (session, connection) in recoveredSessions)
		{
			// bypass MySqlConnection.Dispose(Async), because it's a dummy MySqlConnection that's not set up
			// properly, and simply return the session to the pool directly
			await session.ReturnToPoolAsync(ioBehavior, null).ConfigureAwait(false);

			// be explicit about keeping the associated MySqlConnection alive until the session has been returned
			GC.KeepAlive(connection);
		}
	}

	private async Task CleanPoolAsync(IOBehavior ioBehavior, Func<ServerSession, bool> shouldCleanFn, bool respectMinPoolSize, CancellationToken cancellationToken)
	{
		// synchronize access to this method as only one clean routine should be run at a time
		if (ioBehavior == IOBehavior.Asynchronous)
			await m_cleanSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
		else
			m_cleanSemaphore.Wait(cancellationToken);

		try
		{
			var waitTimeout = TimeSpan.FromMilliseconds(10);
			while (true)
			{
				// if respectMinPoolSize is true, return if (leased sessions + waiting sessions <= minPoolSize)
				if (respectMinPoolSize)
				{
					lock (m_sessions)
					{
						if (ConnectionSettings.MaximumPoolSize - m_sessionSemaphore.CurrentCount + m_sessions.Count <= ConnectionSettings.MinimumPoolSize)
							return;
					}
				}

				// try to get an open slot; if this fails, connection pool is full and sessions will be disposed when returned to pool
				if (ioBehavior == IOBehavior.Asynchronous)
				{
					if (!await m_sessionSemaphore.WaitAsync(waitTimeout, cancellationToken).ConfigureAwait(false))
						return;
				}
				else
				{
					if (!m_sessionSemaphore.Wait(waitTimeout, cancellationToken))
						return;
				}

				try
				{
					// check for a waiting session
					ServerSession? session = null;
					lock (m_sessions)
					{
						if (m_sessions.Count > 0)
						{
							// NOTE: s_connectionsUsageCounter updated outside lock below
							session = m_sessions.Last!.Value;
							m_sessions.RemoveLast();
						}
					}
					if (session is null)
						return;
					s_connectionsUsageCounter.Add(-1, IdleStateTagList);

					if (shouldCleanFn(session))
					{
						// session should be cleaned; dispose it and keep iterating
						Log.FoundSessionToCleanUp(m_logger, Id, session.Id);
						await session.DisposeAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
					}
					else
					{
						// session should not be cleaned; put it back in the queue and stop iterating
						lock (m_sessions)
							m_sessions.AddLast(session);
						s_connectionsUsageCounter.Add(1, IdleStateTagList);
						return;
					}
				}
				finally
				{
					m_sessionSemaphore.Release();
				}
			}
		}
		finally
		{
			m_cleanSemaphore.Release();
		}
	}

	private async Task CreateMinimumPooledSessions(MySqlConnection connection, IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		while (true)
		{
			lock (m_sessions)
			{
				// check if the desired minimum number of sessions have been created
				if (ConnectionSettings.MaximumPoolSize - m_sessionSemaphore.CurrentCount + m_sessions.Count >= ConnectionSettings.MinimumPoolSize)
					return;
			}

			// acquire the semaphore, to ensure that the maximum number of sessions isn't exceeded; if it can't be acquired,
			// we have reached the maximum number of sessions and no more need to be created
			if (ioBehavior == IOBehavior.Asynchronous)
			{
				if (!await m_sessionSemaphore.WaitAsync(0, cancellationToken).ConfigureAwait(false))
					return;
			}
			else
			{
				if (!m_sessionSemaphore.Wait(0, cancellationToken))
					return;
			}

			try
			{
				var session = await ConnectSessionAsync(connection, s_createdToReachMinimumPoolSize, Environment.TickCount, null, ioBehavior, cancellationToken).ConfigureAwait(false);
				AdjustHostConnectionCount(session, 1);
				lock (m_sessions)
					m_sessions.AddFirst(session);
				s_connectionsUsageCounter.Add(1, IdleStateTagList);
			}
			finally
			{
				// connection is in pool; semaphore shouldn't be held any more
				m_sessionSemaphore.Release();
			}
		}
	}

	private async ValueTask<ServerSession> ConnectSessionAsync(MySqlConnection connection, Action<ILogger, int, string, Exception?> logMessage, int startTickCount, Activity? activity, IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		var session = new ServerSession(m_connectionLogger, this, m_generation, Interlocked.Increment(ref m_lastSessionId));
		if (m_logger.IsEnabled(LogLevel.Debug))
			logMessage(m_logger, Id, session.Id, null);
		string? statusInfo;
		try
		{
			statusInfo = await session.ConnectAsync(ConnectionSettings, connection, startTickCount, m_loadBalancer, activity, ioBehavior, cancellationToken).ConfigureAwait(false);
		}
		catch (Exception)
		{
			await session.DisposeAsync(ioBehavior, default).ConfigureAwait(false);
			throw;
		}

		Exception? redirectionException = null;
		if (statusInfo is not null && statusInfo.StartsWith("Location: mysql://", StringComparison.Ordinal))
		{
			// server redirection string has the format "Location: mysql://{host}:{port}/user={userId}[&ttl={ttl}]"
			Log.HasServerRedirectionHeader(m_logger, session.Id, statusInfo);

			if (ConnectionSettings.ServerRedirectionMode == MySqlServerRedirectionMode.Disabled)
			{
				Log.ServerRedirectionIsDisabled(m_logger, Id);
			}
			else if (Utility.TryParseRedirectionHeader(statusInfo, out var host, out var port, out var user))
			{
				if (host != ConnectionSettings.HostNames![0] || port != ConnectionSettings.Port || user != ConnectionSettings.UserID)
				{
					var redirectedSettings = ConnectionSettings.CloneWith(host, port, user);
					Log.OpeningNewConnection(m_logger, Id, host, port, user);
					var redirectedSession = new ServerSession(m_connectionLogger, this, m_generation, Interlocked.Increment(ref m_lastSessionId));
					try
					{
						await redirectedSession.ConnectAsync(redirectedSettings, connection, startTickCount, m_loadBalancer, activity, ioBehavior, cancellationToken).ConfigureAwait(false);
					}
					catch (Exception ex)
					{
						Log.FailedToConnectRedirectedSession(m_logger, ex, Id, redirectedSession.Id);
						redirectionException = ex;
					}

					if (redirectionException is null)
					{
						Log.ClosingSessionToUseRedirectedSession(m_logger, Id, session.Id, redirectedSession.Id);
						await session.DisposeAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
						return redirectedSession;
					}
					else
					{
						try
						{
							await redirectedSession.DisposeAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
						}
						catch (Exception)
						{
						}
					}
				}
				else
				{
					Log.SessionAlreadyConnectedToServer(m_logger, session.Id);
				}
			}
		}

		if (ConnectionSettings.ServerRedirectionMode == MySqlServerRedirectionMode.Required)
		{
			Log.RequiresServerRedirection(m_logger, Id);
			throw new MySqlException(MySqlErrorCode.UnableToConnectToHost, "Server does not support redirection", redirectionException);
		}
		return session;
	}

	public static ConnectionPool? CreatePool(string connectionString, MySqlConnectorLoggingConfiguration loggingConfiguration, string? name)
	{
		// parse connection string and check for 'Pooling' setting; return 'null' if pooling is disabled
		var connectionStringBuilder = new MySqlConnectionStringBuilder(connectionString);
		if (!connectionStringBuilder.Pooling)
			return null;

		// force a new pool to be created, ignoring the cache
		var connectionSettings = new ConnectionSettings(connectionStringBuilder);
		var pool = new ConnectionPool(loggingConfiguration, connectionSettings, name);
		pool.StartReaperTask();
		pool.StartDnsCheckTimer();
		return pool;
	}

	// Gets an existing (unnamed) ConnectionPool, creating it if it's missing. If 'createIfNotFound' is false, then 'loggingConfiguration'
	// may be set to null; otherwise, it must be provided.
	public static ConnectionPool? GetPool(string connectionString, MySqlConnectorLoggingConfiguration? loggingConfiguration, bool createIfNotFound = true)
	{
		// check single-entry MRU cache for this exact connection string; most applications have just one
		// connection string and will get a cache hit here
		var cache = s_mruCache;
		if (cache?.ConnectionString == connectionString)
			return cache.Pool;

		// check if pool has already been created for this exact connection string
		if (s_pools.TryGetValue(connectionString, out var pool))
		{
			s_mruCache = new(connectionString, pool);
			return pool;
		}

		// parse connection string and check for 'Pooling' setting; return 'null' if pooling is disabled
		var connectionStringBuilder = new MySqlConnectionStringBuilder(connectionString);
		if (!connectionStringBuilder.Pooling)
		{
			s_pools.GetOrAdd(connectionString, default(ConnectionPool));
			s_mruCache = new(connectionString, null);
			return null;
		}

		// check for pool using normalized form of connection string
		var normalizedConnectionString = connectionStringBuilder.ConnectionString;
		if (normalizedConnectionString != connectionString && s_pools.TryGetValue(normalizedConnectionString, out pool))
		{
			// try to set the pool for the connection string to the canonical pool; if someone else
			// beats us to it, just use the existing value
			pool = s_pools.GetOrAdd(connectionString, pool)!;
			s_mruCache = new(connectionString, pool);
			return pool;
		}

		if (!createIfNotFound)
			return null;

		// create a new pool and attempt to insert it; if someone else beats us to it, just use their value
		var connectionSettings = new ConnectionSettings(connectionStringBuilder);
		var newPool = new ConnectionPool(loggingConfiguration!, connectionSettings, name: null);
		pool = s_pools.GetOrAdd(normalizedConnectionString, newPool);

		if (pool == newPool)
		{
			s_mruCache = new(connectionString, pool);
			pool.StartReaperTask();
			pool.StartDnsCheckTimer();

			// if we won the race to create the new pool, also store it under the original connection string
			if (connectionString != normalizedConnectionString)
				s_pools.GetOrAdd(connectionString, pool);
		}
		else if (pool != newPool)
		{
			Log.CreatedPoolWillNotBeUsed(newPool.m_logger, newPool.Id);
		}

		return pool;
	}

	public static async Task ClearPoolsAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		foreach (var pool in GetAllPools())
			await pool.ClearAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
	}

	private static IReadOnlyList<ConnectionPool> GetAllPools()
	{
		var pools = new List<ConnectionPool>(s_pools.Count);
		var uniquePools = new HashSet<ConnectionPool>();
		foreach (var pool in s_pools.Values)
		{
			if (pool is not null && uniquePools.Add(pool))
				pools.Add(pool);
		}
		return pools;
	}

	private ConnectionPool(MySqlConnectorLoggingConfiguration loggingConfiguration, ConnectionSettings cs, string? name)
	{
		m_logger = loggingConfiguration.PoolLogger;
		m_connectionLogger = loggingConfiguration.ConnectionLogger;
		ConnectionSettings = cs;
		Name = name;
		SslProtocols = cs.TlsVersions;
		m_generation = 0;
		m_cleanSemaphore = new(1);
		m_sessionSemaphore = new(cs.MaximumPoolSize);
		m_sessions = new();
		m_leasedSessions = new();
		if (cs.ConnectionProtocol == MySqlConnectionProtocol.Sockets && cs.LoadBalance == MySqlLoadBalance.LeastConnections)
		{
			m_hostSessions = new();
			foreach (var hostName in cs.HostNames!)
				m_hostSessions[hostName] = 0;
		}

		m_loadBalancer = cs.ConnectionProtocol != MySqlConnectionProtocol.Sockets ? null :
			cs.HostNames!.Count == 1 || cs.LoadBalance == MySqlLoadBalance.FailOver ? FailOverLoadBalancer.Instance :
			cs.LoadBalance == MySqlLoadBalance.Random ? RandomLoadBalancer.Instance :
			cs.LoadBalance == MySqlLoadBalance.LeastConnections ? new LeastConnectionsLoadBalancer(m_hostSessions!) :
			(ILoadBalancer) new RoundRobinLoadBalancer();

		// create tag lists for reporting pool metrics
		var connectionString = cs.ConnectionStringBuilder.GetConnectionString(includePassword: false);
		m_stateTagList = new KeyValuePair<string, object?>[3]
		{
			new("state", "idle"),
			new("pool.name", Name ?? connectionString),
			new("state", "used"),
		};

		// set pool size counters
		s_minIdleConnectionsCounter.Add(ConnectionSettings.MinimumPoolSize, PoolNameTagList);
		s_maxIdleConnectionsCounter.Add(ConnectionSettings.MaximumPoolSize, PoolNameTagList);
		s_maxConnectionsCounter.Add(ConnectionSettings.MaximumPoolSize, PoolNameTagList);

		Id = Interlocked.Increment(ref s_poolId);
		Log.CreatingNewConnectionPool(m_logger, Id, connectionString);
	}

	private void StartReaperTask()
	{
		if (ConnectionSettings.ConnectionIdleTimeout <= 0)
			return;

		var reaperInterval = TimeSpan.FromSeconds(Math.Max(1, Math.Min(60, ConnectionSettings.ConnectionIdleTimeout / 2)));

#if NET6_0_OR_GREATER
		m_reaperTimer = new PeriodicTimer(reaperInterval);
		_ = RunTimer();

		async Task RunTimer()
		{
			while (await m_reaperTimer.WaitForNextTickAsync().ConfigureAwait(false))
			{
				try
				{
					using var source = new CancellationTokenSource(reaperInterval);
					await ReapAsync(IOBehavior.Asynchronous, source.Token).ConfigureAwait(false);
				}
				catch
				{
					// do nothing; we'll try to reap again
				}
			}
		}
#else
		m_reaperTimer = new Timer(t =>
		{
			var stopwatch = Stopwatch.StartNew();
			try
			{
				using var source = new CancellationTokenSource(reaperInterval);
				ReapAsync(IOBehavior.Synchronous, source.Token).GetAwaiter().GetResult();
			}
			catch
			{
				// do nothing; we'll try to reap again
			}

			// restart the timer, accounting for the time spent reaping
			var delay = reaperInterval - stopwatch.Elapsed;
			((Timer) t!).Change(delay < TimeSpan.Zero ? TimeSpan.Zero : delay, TimeSpan.FromMilliseconds(-1));
		});
		m_reaperTimer.Change(reaperInterval, TimeSpan.FromMilliseconds(-1));
#endif
	}

	private void StartDnsCheckTimer()
	{
		if (ConnectionSettings.ConnectionProtocol != MySqlConnectionProtocol.Tcp || ConnectionSettings.DnsCheckInterval <= 0)
			return;

		var hostNames = ConnectionSettings.HostNames!;
		var hostAddresses = new IPAddress[hostNames.Count][];

#if NET6_0_OR_GREATER
		m_dnsCheckTimer = new PeriodicTimer(TimeSpan.FromSeconds(ConnectionSettings.DnsCheckInterval));
		_ = RunTimer();

		async Task RunTimer()
		{
			while (await m_dnsCheckTimer.WaitForNextTickAsync().ConfigureAwait(false))
			{
				Log.CheckingForDnsChanges(m_logger, Id);
				var hostNamesChanged = false;
				for (var hostNameIndex = 0; hostNameIndex < hostNames.Count; hostNameIndex++)
				{
					try
					{
						var ipAddresses = await Dns.GetHostAddressesAsync(hostNames[hostNameIndex]).ConfigureAwait(false);
						if (hostAddresses[hostNameIndex] is null)
						{
							hostAddresses[hostNameIndex] = ipAddresses;
						}
						else if (hostAddresses[hostNameIndex].Except(ipAddresses).Any())
						{
							Log.DetectedDnsChange(m_logger, Id, hostNames[hostNameIndex], string.Join<IPAddress>(',', hostAddresses[hostNameIndex]), string.Join<IPAddress>(',', ipAddresses));
							hostAddresses[hostNameIndex] = ipAddresses;
							hostNamesChanged = true;
						}
					}
					catch (Exception ex)
					{
						// do nothing; we'll try again later
						Log.DnsCheckFailed(m_logger, ex, Id, hostNames[hostNameIndex], ex.Message);
					}
				}
				if (hostNamesChanged)
				{
					Log.ClearingPoolDueToDnsChanges(m_logger, Id);
					await ClearAsync(IOBehavior.Asynchronous, CancellationToken.None).ConfigureAwait(false);
				}
			}
		}
#else
		var interval = Math.Min(int.MaxValue / 1000, ConnectionSettings.DnsCheckInterval) * 1000;
		m_dnsCheckTimer = new Timer(t =>
		{
			Log.CheckingForDnsChanges(m_logger, Id);
			var hostNamesChanged = false;
			for (var hostNameIndex = 0; hostNameIndex < hostNames.Count; hostNameIndex++)
			{
				try
				{
					var ipAddresses = Dns.GetHostAddresses(hostNames[hostNameIndex]);
					if (hostAddresses[hostNameIndex] is null)
					{
						hostAddresses[hostNameIndex] = ipAddresses;
					}
					else if (hostAddresses[hostNameIndex].Except(ipAddresses).Any())
					{
						Log.DetectedDnsChange(m_logger, Id, hostNames[hostNameIndex], string.Join<IPAddress>(",", hostAddresses[hostNameIndex]), string.Join<IPAddress>(",", ipAddresses));
						hostAddresses[hostNameIndex] = ipAddresses;
						hostNamesChanged = true;
					}
				}
				catch (Exception ex)
				{
					// do nothing; we'll try again later
					Log.DnsCheckFailed(m_logger, ex, Id, hostNames[hostNameIndex], ex.Message);
				}
			}
			if (hostNamesChanged)
			{
				Log.ClearingPoolDueToDnsChanges(m_logger, Id);
				ClearAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();
			}
			((Timer) t!).Change(interval, -1);
		});
		m_dnsCheckTimer.Change(interval, -1);
#endif
	}

	private void AdjustHostConnectionCount(ServerSession session, int delta)
	{
		if (m_hostSessions is not null)
		{
			lock (m_hostSessions)
				m_hostSessions[session.HostName] += delta;
		}
	}

	// Provides a slice of m_stateTagList that contains either the 'idle' or 'used' state tag along with the pool name.
	private ReadOnlySpan<KeyValuePair<string, object?>> IdleStateTagList => m_stateTagList.AsSpan(0, 2);
	private ReadOnlySpan<KeyValuePair<string, object?>> UsedStateTagList => m_stateTagList.AsSpan(1, 2);

	// A slice of m_stateTagList that contains only the pool name tag.
	public ReadOnlySpan<KeyValuePair<string, object?>> PoolNameTagList => m_stateTagList.AsSpan(1, 1);

	private sealed class LeastConnectionsLoadBalancer : ILoadBalancer
	{
		public LeastConnectionsLoadBalancer(Dictionary<string, int> hostSessions) => m_hostSessions = hostSessions;

		public IReadOnlyList<string> LoadBalance(IReadOnlyList<string> hosts)
		{
			lock (m_hostSessions)
				return m_hostSessions.OrderBy(static x => x.Value).Select(static x => x.Key).ToList();
		}

		private readonly Dictionary<string, int> m_hostSessions;
	}

	private sealed class ConnectionStringPool
	{
		public ConnectionStringPool(string connectionString, ConnectionPool? pool)
		{
			ConnectionString = connectionString;
			Pool = pool;
		}

		public string ConnectionString { get; }
		public ConnectionPool? Pool { get; }
	}

	static ConnectionPool()
	{
		AppDomain.CurrentDomain.DomainUnload += OnAppDomainShutDown;
		AppDomain.CurrentDomain.ProcessExit += OnAppDomainShutDown;
	}

	private static void OnAppDomainShutDown(object? sender, EventArgs e) =>
		ClearPoolsAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

	private static readonly UpDownCounter<int> s_connectionsUsageCounter = ActivitySourceHelper.Meter.CreateUpDownCounter<int>("db.client.connections.usage",
		unit: "{connection}", description: "The number of connections that are currently in the state described by the state tag.");
	private static readonly UpDownCounter<int> s_maxIdleConnectionsCounter = ActivitySourceHelper.Meter.CreateUpDownCounter<int>("db.client.connections.idle.max",
		unit: "{connection}", description: "The maximum number of idle open connections allowed.");
	private static readonly UpDownCounter<int> s_minIdleConnectionsCounter = ActivitySourceHelper.Meter.CreateUpDownCounter<int>("db.client.connections.idle.min",
		unit: "{connection}", description: "The minimum number of idle open connections allowed.");
	private static readonly UpDownCounter<int> s_maxConnectionsCounter = ActivitySourceHelper.Meter.CreateUpDownCounter<int>("db.client.connections.max",
		unit: "{connection}", description: "The maximum number of open connections allowed.");
	private static readonly UpDownCounter<int> s_pendingRequestsCounter = ActivitySourceHelper.Meter.CreateUpDownCounter<int>("db.client.connections.pending_requests",
		unit: "{request}", description: "The number of pending requests for an open connection, cumulative for the entire pool.");
	private static readonly Histogram<float> s_createTimeHistory = ActivitySourceHelper.Meter.CreateHistogram<float>("db.client.connections.create_time",
		unit: "ms", description: "The time it took to create a new connection.");
	private static readonly Histogram<float> s_waitTimeHistory = ActivitySourceHelper.Meter.CreateHistogram<float>("db.client.connections.wait_time",
		unit: "ms", description: "The time it took to obtain an open connection from the pool.");
	private static readonly ConcurrentDictionary<string, ConnectionPool?> s_pools = new();
	private static readonly Action<ILogger, int, string, Exception?> s_createdNewSession = LoggerMessage.Define<int, string>(
		LogLevel.Debug, new EventId(EventIds.PoolCreatedNewSession, nameof(EventIds.PoolCreatedNewSession)),
		"Pool {PoolId} has no pooled session available; created new session {SessionId}");
	private static readonly Action<ILogger, int, string, Exception?> s_createdToReachMinimumPoolSize = LoggerMessage.Define<int, string>(
		LogLevel.Debug, new EventId(EventIds.CreatedSessionToReachMinimumPoolCount, nameof(EventIds.CreatedSessionToReachMinimumPoolCount)),
		"Pool {PoolId} created session {SessionId} to reach minimum pool size");
	private static int s_poolId;
	private static ConnectionStringPool? s_mruCache;

	private readonly ILogger m_logger;
	private readonly ILogger m_connectionLogger;
	private readonly KeyValuePair<string, object?>[] m_stateTagList;
	private readonly SemaphoreSlim m_cleanSemaphore;
	private readonly SemaphoreSlim m_sessionSemaphore;
	private readonly LinkedList<ServerSession> m_sessions;
	private readonly Dictionary<string, ServerSession> m_leasedSessions;
	private readonly ILoadBalancer? m_loadBalancer;
	private readonly Dictionary<string, int>? m_hostSessions;
	private int m_generation;
	private uint m_lastRecoveryTime;
	private int m_lastSessionId;
	private Dictionary<string, CachedProcedure?>? m_procedureCache;
#if NET6_0_OR_GREATER
	private PeriodicTimer? m_dnsCheckTimer;
	private PeriodicTimer? m_reaperTimer;
#else
	private Timer? m_dnsCheckTimer;
	private Timer? m_reaperTimer;
#endif
}
