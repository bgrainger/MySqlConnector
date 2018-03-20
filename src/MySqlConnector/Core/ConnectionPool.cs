using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Logging;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Core
{
	internal sealed class ConnectionPool
	{
		public int Id { get; }

		public ConnectionSettings ConnectionSettings { get; }

		public async ValueTask<ServerSession> GetSessionAsync(MySqlConnection connection, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			cancellationToken.ThrowIfCancellationRequested();

			if (ConnectionSettings.MinimumPoolSize > 0 && !m_initialized)
			{
				lock (this)
				{
					if (!m_initialized)
					{
						CreateMinimumPooledSessions(ioBehavior, cancellationToken).ConfigureAwait(false);
						m_initialized = true;
					}
				}
			}

			ServerSession session;

			var start = Thread.CurrentThread.ManagedThreadId;
			for (var i = 0; i < m_sessions.Length; i++)
			{
				var j = (i + start) % m_sessions.Length;
				session = m_sessions[j];
				if (session != null && Interlocked.CompareExchange(ref m_sessions[j], null, session) == session)
				{
					Log.Debug("{0} found an existing session; checking it for validity", m_logArguments);
					bool reuseSession;

					if (session.PoolGeneration != m_generation)
					{
						Log.Debug("{0} discarding session due to wrong generation", m_logArguments);
						reuseSession = false;
					}
					else
					{
						if (ConnectionSettings.ConnectionReset)
						{
							reuseSession = await session.TryResetConnectionAsync(ConnectionSettings, ioBehavior, cancellationToken).ConfigureAwait(false);
						}
						else if (unchecked((uint) Environment.TickCount) - session.LastReturned > 5000)
						{
							reuseSession = await session.TryPingAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
						}
						else
						{
							reuseSession = true;
						}
					}

					if (!reuseSession)
					{
						// session is either old or cannot communicate with the server
						Log.Warn("{0} Session{1} is unusable; destroying it", m_logArguments[0], session.Id);
						await session.DisposeAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
					}
					else
					{
						// pooled session is ready to be used; return it
						session.OwningConnection = new WeakReference<MySqlConnection>(connection);
						return session;
					}

					return session;
				}
			}


			// create a new session
			session = new ServerSession(this, m_generation, Interlocked.Increment(ref m_lastSessionId));
			if (Log.IsInfoEnabled())
				Log.Info("{0} no pooled session available; created new Session{1}", m_logArguments[0], session.Id);
			await session.ConnectAsync(ConnectionSettings, m_loadBalancer, ioBehavior, cancellationToken).ConfigureAwait(false);
			session.OwningConnection = new WeakReference<MySqlConnection>(connection);

			return session;
		}

		private bool SessionIsHealthy(ServerSession session)
		{
			if (!session.IsConnected)
				return false;
			if (session.PoolGeneration != m_generation)
				return false;
			if (session.DatabaseOverride != null)
				return false;
			if (ConnectionSettings.ConnectionLifeTime > 0
				&& (DateTime.UtcNow - session.CreatedUtc).TotalSeconds >= ConnectionSettings.ConnectionLifeTime)
				return false;

			return true;
		}

		public void Return(ServerSession session)
		{
			if (Log.IsDebugEnabled())
				Log.Debug("{0} receiving Session{1} back", m_logArguments[0], session.Id);


			session.OwningConnection = null;
			if (SessionIsHealthy(session))
			{
				var start = Thread.CurrentThread.ManagedThreadId;
				for (var i = 0; i < m_sessions.Length; i++)
				{
					var j = (i + start) % m_sessions.Length;
					if (m_sessions[j] == null && Interlocked.CompareExchange(ref m_sessions[j], session, null) == null)
					{
						return;
					}
				}
			}

			session.DisposeAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();
		}

		public async Task ClearAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			// increment the generation of the connection pool
			Log.Info("{0} clearing connection pool", m_logArguments);
			Interlocked.Increment(ref m_generation);
			m_procedureCache = null;

			await CleanPoolAsync(ioBehavior, session => session.PoolGeneration != m_generation, false, cancellationToken).ConfigureAwait(false);
		}
		private async Task CleanPoolAsync(IOBehavior ioBehavior, Func<ServerSession, bool> shouldCleanFn, bool respectMinPoolSize, CancellationToken cancellationToken)
		{
			foreach (var session in m_sessions)
			{
				if (session != null)
				{
					await session.DisposeAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				}
			}
		}

		/// <summary>
		/// Returns the stored procedure cache for this <see cref="ConnectionPool"/>, lazily creating it on demand.
		/// This method may return a different object after <see cref="ClearAsync"/> has been called. The returned
		/// object is shared between multiple threads and is only safe to use after taking a <c>lock</c> on the
		/// object itself.
		/// </summary>
		public Dictionary<string, CachedProcedure> GetProcedureCache()
		{
			var procedureCache = m_procedureCache;
			if (procedureCache == null)
			{
				var newProcedureCache = new Dictionary<string, CachedProcedure>();
				procedureCache = Interlocked.CompareExchange(ref m_procedureCache, newProcedureCache, null) ?? newProcedureCache;
			}
			return procedureCache;
		}

		private async Task CreateMinimumPooledSessions(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			for (var i = 0; i < Math.Max(ConnectionSettings.MinimumPoolSize, m_sessions.Length); i++)
			{
				var session = new ServerSession(this, m_generation, Interlocked.Increment(ref m_lastSessionId));
				Log.Info("{0} created Session{1} to reach minimum pool size", m_logArguments[0], session.Id);
				await session.ConnectAsync(ConnectionSettings, m_loadBalancer, ioBehavior, cancellationToken).ConfigureAwait(false);
				m_sessions[i] = session;
			}
		}

		public static ConnectionPool GetPool(string connectionString)
		{
			// check single-entry MRU cache for this exact connection string; most applications have just one
			// connection string and will get a cache hit here
			var cache = s_mruCache;
			if (cache?.ConnectionString == connectionString)
				return cache.Pool;

			// check if pool has already been created for this exact connection string
			if (s_pools.TryGetValue(connectionString, out var pool))
			{
				s_mruCache = new ConnectionStringPool(connectionString, pool);
				return pool;
			}

			// parse connection string and check for 'Pooling' setting; return 'null' if pooling is disabled
			var connectionStringBuilder = new MySqlConnectionStringBuilder(connectionString);
			if (!connectionStringBuilder.Pooling)
			{
				s_pools.GetOrAdd(connectionString, default(ConnectionPool));
				s_mruCache = new ConnectionStringPool(connectionString, null);
				return null;
			}

			// check for pool using normalized form of connection string
			var normalizedConnectionString = connectionStringBuilder.ConnectionString;
			if (normalizedConnectionString != connectionString && s_pools.TryGetValue(normalizedConnectionString, out pool))
			{
				// try to set the pool for the connection string to the canonical pool; if someone else
				// beats us to it, just use the existing value
				pool = s_pools.GetOrAdd(connectionString, pool);
				s_mruCache = new ConnectionStringPool(connectionString, pool);
				return pool;
			}

			// create a new pool and attempt to insert it; if someone else beats us to it, just use their value
			var connectionSettings = new ConnectionSettings(connectionStringBuilder);
			var newPool = new ConnectionPool(connectionSettings);
			pool = s_pools.GetOrAdd(normalizedConnectionString, newPool);

			// if we won the race to create the new pool, also store it under the original connection string
			if (pool == newPool && connectionString != normalizedConnectionString)
			{
				s_pools.GetOrAdd(connectionString, pool);
				s_mruCache = new ConnectionStringPool(connectionString, pool);
			}
			else if (pool != newPool && Log.IsInfoEnabled())
			{
				Log.Info("{0} was created but will not be used (due to race)", newPool.m_logArguments[0]);
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
				if (pool != null && uniquePools.Add(pool))
					pools.Add(pool);
			}
			return pools;
		}

		private ConnectionPool(ConnectionSettings cs)
		{
			ConnectionSettings = cs;
			m_generation = 0;
			m_sessions = new ServerSession[ConnectionSettings.MaximumPoolSize];

			m_loadBalancer = cs.ConnectionType != ConnectionType.Tcp ? null : FailOverLoadBalancer.Instance;

			Id = Interlocked.Increment(ref s_poolId);
			m_logArguments = new object[] { "Pool{0}".FormatInvariant(Id) };
			if (Log.IsInfoEnabled())
				Log.Info("{0} creating new connection pool for {1}", m_logArguments[0], cs.ConnectionStringBuilder.GetConnectionString(includePassword: false));
		}

		private sealed class ConnectionStringPool
		{
			public ConnectionStringPool(string connectionString, ConnectionPool pool)
			{
				ConnectionString = connectionString;
				Pool = pool;
			}

			public string ConnectionString { get; }
			public ConnectionPool Pool { get; }
		}

		static readonly IMySqlConnectorLogger Log = MySqlConnectorLogManager.CreateLogger(nameof(ConnectionPool));
		static readonly ConcurrentDictionary<string, ConnectionPool> s_pools = new ConcurrentDictionary<string, ConnectionPool>();

		static int s_poolId;
		static ConnectionStringPool s_mruCache;

		int m_generation;
		bool m_initialized = false;
		//readonly SemaphoreSlim m_cleanSemaphore;
		//readonly SemaphoreSlim m_sessionSemaphore;
		readonly ServerSession[] m_sessions;
		//readonly Dictionary<string, ServerSession> m_leasedSessions;
		readonly ILoadBalancer m_loadBalancer;
		//readonly Dictionary<string, int> m_hostSessions;
		readonly object[] m_logArguments;
		//readonly Task m_reaperTask;
		//uint m_lastRecoveryTime;
		int m_lastSessionId;
		Dictionary<string, CachedProcedure> m_procedureCache;
	}
}
