using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Xunit;

namespace SideBySide
{
	public class ConnectionPool : IClassFixture<DatabaseFixture>
	{
		[Theory]
		[InlineData(false, 11, 0L)]
		[InlineData(true, 12, 1L)]
#if BASELINE
		// baseline default behaviour is to not reset the connection, which trades correctness for speed
		// see bug report at http://bugs.mysql.com/bug.php?id=77421
		[InlineData(null, 13, 0L)]
#else
		[InlineData(null, 13, 1L)]
#endif
		public void ResetConnection(object connectionReset, int poolSize, long expected)
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.Pooling = true;
			csb.MaximumPoolSize = (uint) poolSize; // use a different pool size to create a unique connection string to force a unique pool to be created

			if (connectionReset != null)
				csb.ConnectionReset = (bool) connectionReset;

			using (var connection = new MySqlConnection(csb.ConnectionString))
			{
				connection.Open();
				using var command = connection.CreateCommand();
				command.CommandText = "select @@autocommit;";
				Assert.Equal(1L, command.ExecuteScalar());
			}

			using (var connection = new MySqlConnection(csb.ConnectionString))
			{
				connection.Open();
				using var command = connection.CreateCommand();
				command.CommandText = "SET autocommit=0;";
				command.ExecuteNonQuery();
			}

			using (var connection = new MySqlConnection(csb.ConnectionString))
			{
				connection.Open();
				using var command = connection.CreateCommand();
				command.CommandText = "select @@autocommit;";
				Assert.Equal(expected, command.ExecuteScalar());
			}
		}

		[Fact]
		public async Task ExhaustConnectionPool()
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.Pooling = true;
			csb.MinimumPoolSize = 0;
			csb.MaximumPoolSize = 3;
			csb.ConnectionTimeout = 60;

			var connections = new List<MySqlConnection>();

			for (int i = 0; i < csb.MaximumPoolSize; i++)
			{
				var connection = new MySqlConnection(csb.ConnectionString);
				await connection.OpenAsync().ConfigureAwait(false);
				connections.Add(connection);
			}

			var closeTask = Task.Run(() =>
			{
				Thread.Sleep(5000);
				connections[0].Dispose();
				connections.RemoveAt(0);
			});

			using (var extraConnection = new MySqlConnection(csb.ConnectionString))
			{
				var stopwatch = Stopwatch.StartNew();
				await extraConnection.OpenAsync().ConfigureAwait(false);
				stopwatch.Stop();
				Assert.InRange(stopwatch.ElapsedMilliseconds, 4500, 7500);
			}

			closeTask.Wait();

			foreach (var connection in connections)
				connection.Dispose();
		}

		[Fact]
		public async Task ExhaustConnectionPoolWithTimeout()
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.Pooling = true;
			csb.MinimumPoolSize = 0;
			csb.MaximumPoolSize = 3;
			csb.ConnectionTimeout = 5;

			var connections = new List<MySqlConnection>();

			for (int i = 0; i < csb.MaximumPoolSize; i++)
			{
				var connection = new MySqlConnection(csb.ConnectionString);
				await connection.OpenAsync().ConfigureAwait(false);
				connections.Add(connection);
			}

			using (var extraConnection = new MySqlConnection(csb.ConnectionString))
			{
				var stopwatch = Stopwatch.StartNew();
				Assert.Throws<MySqlException>(() => extraConnection.Open());
				stopwatch.Stop();
				Assert.InRange(stopwatch.ElapsedMilliseconds, 4500, 6000);
			}

			foreach (var connection in connections)
				connection.Dispose();
		}

		[Fact]
		public void LeakConnections()
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.Pooling = true;
			csb.MinimumPoolSize = 0;
			csb.MaximumPoolSize = 6;
			csb.ConnectionTimeout = 3u;

			for (int i = 0; i < csb.MaximumPoolSize + 2; i++)
			{
				var connection = new MySqlConnection(csb.ConnectionString);
				connection.Open();

				// have to GC for leaked connections to be removed from the pool
				GC.Collect();
				Thread.Sleep(400);
			}
		}

		[Fact]
		public async Task WaitTimeout()
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.Pooling = true;
			csb.MinimumPoolSize = 0;
			csb.MaximumPoolSize = 1;
			int serverThread;

			using (var connection = new MySqlConnection(csb.ConnectionString))
			{
				await connection.OpenAsync();
				using (var cmd = connection.CreateCommand())
				{
					cmd.CommandText = "SET @@session.wait_timeout=3";
					await cmd.ExecuteNonQueryAsync();
				}
				serverThread = connection.ServerThread;
			}

			await Task.Delay(TimeSpan.FromSeconds(5));

			using (var connection = new MySqlConnection(csb.ConnectionString))
			{
				await connection.OpenAsync();
				Assert.NotEqual(serverThread, connection.ServerThread);
			}
		}

		[Fact]
		public async Task CharacterSet()
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.Pooling = true;
			csb.MinimumPoolSize = 0;
			csb.MaximumPoolSize = 21; // use a uniqe pool size to create a unique connection string to force a unique pool to be created
#if BASELINE
			csb.CharacterSet = "utf8mb4";
#endif

			// verify that connection charset is the same when retrieving a connection from the pool
			await CheckCharacterSetAsync(csb.ConnectionString).ConfigureAwait(false);
			await CheckCharacterSetAsync(csb.ConnectionString).ConfigureAwait(false);
			await CheckCharacterSetAsync(csb.ConnectionString).ConfigureAwait(false);
		}

		private async Task CheckCharacterSetAsync(string connectionString)
		{
			using var connection = new MySqlConnection(connectionString);
			await connection.OpenAsync().ConfigureAwait(false);
			using var cmd = connection.CreateCommand();
			cmd.CommandText = @"select @@character_set_client, @@character_set_connection";
			using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
			Assert.True(await reader.ReadAsync().ConfigureAwait(false));
			Assert.Equal("utf8mb4", reader.GetString(0));
			Assert.Equal("utf8mb4", reader.GetString(1));
			Assert.False(await reader.ReadAsync().ConfigureAwait(false));
		}

		[Fact]
		public async Task ClearConnectionPool()
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.Pooling = true;
			csb.MinimumPoolSize = 0;
			csb.MaximumPoolSize = 3;

			var connections = new List<MySqlConnection>();

			for (int i = 0; i < csb.MaximumPoolSize; i++)
			{
				var connection = new MySqlConnection(csb.ConnectionString);
				connections.Add(connection);
			}

			Func<HashSet<long>> getConnectionIds = () =>
			{
				var cids = GetConnectionIds(connections);
				Assert.Equal(connections.Count, cids.Count);
				return cids;
			};

			Func<Task> openConnections = async () =>
			{
				foreach (var connection in connections)
				{
					await connection.OpenAsync();
				}
			};

			Action closeConnections = () =>
			{
				foreach (var connection in connections)
				{
					connection.Close();
				}
			};

			// connections should all be disposed when returned to pool
			await openConnections();
			var connectionIds = getConnectionIds();
			await ClearPoolAsync(connections[0]);
			closeConnections();
			await openConnections();
			var connectionIds2 = getConnectionIds();
			Assert.Empty(connectionIds.Intersect(connectionIds2));
			closeConnections();

			// connections should all be disposed in ClearPoolAsync
			await ClearPoolAsync(connections[0]);
			await openConnections();
			var connectionIds3 = getConnectionIds();
			Assert.Empty(connectionIds2.Intersect(connectionIds3));
			closeConnections();

			// some connections may be disposed in ClearPoolAsync, others in OpenAsync
			var clearTask = ClearPoolAsync(connections[0]);
			await openConnections();
			var connectionIds4 = GetConnectionIds(connections);
			Assert.Empty(connectionIds3.Intersect(connectionIds4));
			await clearTask;
			closeConnections();

			foreach (var connection in connections)
				connection.Dispose();
		}

#if !BASELINE
		[Theory]
		[InlineData(1u, 3u, 0u, 5u)]
		[InlineData(1u, 3u, 3u, 5u)]
		public async Task ConnectionLifeTime(uint idleTimeout, uint delaySeconds, uint minPoolSize, uint maxPoolSize)
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.Pooling = true;
			csb.MinimumPoolSize = minPoolSize;
			csb.MaximumPoolSize = maxPoolSize;
			csb.ConnectionIdleTimeout = idleTimeout;
			HashSet<int> serverThreadIdsBegin = new HashSet<int>();
			HashSet<int> serverThreadIdsEnd = new HashSet<int>();

			async Task OpenConnections(uint numConnections, HashSet<int> serverIdSet)
			{
				using var connection = new MySqlConnection(csb.ConnectionString);
				await connection.OpenAsync();
				serverIdSet.Add(connection.ServerThread);
				if (--numConnections <= 0)
					return;
				await OpenConnections(numConnections, serverIdSet);
			}

			await OpenConnections(maxPoolSize, serverThreadIdsBegin);
			await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
			await OpenConnections(maxPoolSize, serverThreadIdsEnd);

			serverThreadIdsEnd.IntersectWith(serverThreadIdsBegin);
			Assert.Equal((int) minPoolSize, serverThreadIdsEnd.Count);
		}
#endif

		private Task ClearPoolAsync(MySqlConnection connection)
		{
#if BASELINE
			return connection.ClearPoolAsync(connection);
#else
			return MySqlConnection.ClearPoolAsync(connection);
#endif
		}

		private static HashSet<long> GetConnectionIds(IEnumerable<MySqlConnection> connections)
			=> new HashSet<long>(connections.Select(x => (long) x.ServerThread));
	}
}
