namespace MySqlConnector.Tests;

public class ConnectionTests : IDisposable
{
	public ConnectionTests()
	{
		m_server = new();
		m_server.Start();

		m_csb = new()
		{
			Server = "localhost",
			Port = (uint) m_server.Port,
		};
	}

	public void Dispose()
	{
		m_server.Stop();
	}

	[Fact]
	public void PooledConnectionIsReturnedToPool()
	{
		Assert.Equal(0, m_server.ActiveConnections);

		m_csb.Pooling = true;
		using (var connection = new MySqlConnection(m_csb.ConnectionString))
		{
			connection.Open();
			Assert.Equal(1, m_server.ActiveConnections);

			Assert.Equal(m_server.ServerVersion, connection.ServerVersion);
			connection.Close();
			Assert.Equal(1, m_server.ActiveConnections);
		}

		Assert.Equal(1, m_server.ActiveConnections);
	}

	[Fact]
	public async Task PooledConnectionIsReturnedToPoolAsync()
	{
		Assert.Equal(0, m_server.ActiveConnections);

		m_csb.Pooling = true;
		using (var connection = new MySqlConnection(m_csb.ConnectionString))
		{
			await connection.OpenAsync();
			Assert.Equal(1, m_server.ActiveConnections);

			Assert.Equal(m_server.ServerVersion, connection.ServerVersion);
			await connection.CloseAsync();
			Assert.Equal(1, m_server.ActiveConnections);
		}

		Assert.Equal(1, m_server.ActiveConnections);
	}

	[Fact]
	public async Task UnpooledConnectionIsClosed()
	{
		Assert.Equal(0, m_server.ActiveConnections);

		m_csb.Pooling = false;
		using var connection = new MySqlConnection(m_csb.ConnectionString);
		await connection.OpenAsync();
		Assert.Equal(1, m_server.ActiveConnections);

		Assert.Equal(m_server.ServerVersion, connection.ServerVersion);
		connection.Close();

		await WaitForConditionAsync(0, () => m_server.ActiveConnections);
	}

	[Theory]
	[InlineData(2u, 3u, true)]
	[InlineData(180u, 3u, false)]
	public async Task ConnectionLifeTime(uint lifeTime, uint delaySeconds, bool shouldTimeout)
	{
		m_csb.Pooling = true;
		m_csb.MinimumPoolSize = 0;
		m_csb.MaximumPoolSize = 1;
		m_csb.ConnectionLifeTime = lifeTime;
		int serverThread;

		using (var connection = new MySqlConnection(m_csb.ConnectionString))
		{
			await connection.OpenAsync();
			serverThread = connection.ServerThread;
			await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
		}

		using (var connection = new MySqlConnection(m_csb.ConnectionString))
		{
			await connection.OpenAsync();
			if (shouldTimeout)
				Assert.NotEqual(serverThread, connection.ServerThread);
			else
				Assert.Equal(serverThread, connection.ServerThread);
		}
	}

	[Theory]
	[InlineData(3)]
	[InlineData(7)]
	public async Task MinimumPoolSize(int size)
	{
		m_csb.MinimumPoolSize = (uint) size;
		using var connection = new MySqlConnection(m_csb.ConnectionString);
		await connection.OpenAsync();
		Assert.Equal(size, m_server.ActiveConnections);
	}

	[Fact]
	public void LeakReaders()
	{
		m_csb.Pooling = true;
		m_csb.MinimumPoolSize = 0;
		m_csb.MaximumPoolSize = 6;
		m_csb.ConnectionTimeout = 30u;

		for (var i = 0; i < m_csb.MaximumPoolSize + 2; i++)
		{
			var connection = new MySqlConnection(m_csb.ConnectionString);
			connection.Open();

			var cmd = connection.CreateCommand();
			cmd.CommandText = "SELECT 1;";
			using (var reader = cmd.ExecuteReader())
				Assert.True(reader.Read());

			// have to GC for leaked connections to be removed from the pool
			GC.Collect();

			// HACK: have to sleep (so that RecoverLeakedSessions is called in ConnectionPool.GetSessionAsync)
			Thread.Sleep(250);
		}
	}

	[Fact]
	public void AuthPluginNameNotNullTerminated()
	{
		m_server.SuppressAuthPluginNameTerminatingNull = true;
		using var connection = new MySqlConnection(m_csb.ConnectionString);
		connection.Open();
		Assert.Equal(ConnectionState.Open, connection.State);
	}

	[Fact]
	public void IncompleteServerHandshake()
	{
		m_server.SendIncompletePostHandshakeResponse = true;
		using var connection = new MySqlConnection(m_csb.ConnectionString);
		Assert.Throws<MySqlException>(() => connection.Open());
	}

	[Fact]
	public void Ping()
	{
		using var connection = new MySqlConnection(m_csb.ConnectionString);
		connection.Open();
		Assert.Equal(ConnectionState.Open, connection.State);
		Assert.True(connection.Ping());
		Assert.Equal(ConnectionState.Open, connection.State);
	}

	[Fact]
	public void PingWhenClosed()
	{
		using var connection = new MySqlConnection(m_csb.ConnectionString);
		connection.Open();
		Assert.Equal(ConnectionState.Open, connection.State);
		m_server.Stop();
		Assert.False(connection.Ping());
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Fact]
	public void ConnectionTimeout()
	{
		m_server.ConnectDelay = TimeSpan.FromSeconds(10);
		var csb = new MySqlConnectionStringBuilder(m_csb.ConnectionString);
		csb.ConnectionTimeout = 4;
		using var connection = new MySqlConnection(csb.ConnectionString);
		var stopwatch = Stopwatch.StartNew();
		try
		{
			connection.Open();
			Assert.False(true);
		}
		catch (MySqlException ex)
		{
			Assert.InRange(stopwatch.ElapsedMilliseconds, 3900, 4100);
			Assert.Equal(MySqlErrorCode.UnableToConnectToHost, (MySqlErrorCode) ex.Number);
		}
	}

	[Fact]
	public void ReadInfinity()
	{
		using var connection = new MySqlConnection(m_csb.ConnectionString);
		connection.Open();
		using var command = new MySqlCommand("select infinity", connection);
		using var reader = command.ExecuteReader();
		Assert.True(reader.Read());
		Assert.Equal(float.NaN, reader.GetValue(0));
		Assert.Equal(double.NaN, reader.GetValue(1));
		Assert.True(reader.Read());
		Assert.Equal(float.PositiveInfinity, reader.GetValue(0));
		Assert.Equal(double.PositiveInfinity, reader.GetValue(1));
		Assert.True(reader.Read());
		Assert.Equal(float.NegativeInfinity, reader.GetValue(0));
		Assert.Equal(double.NegativeInfinity, reader.GetValue(1));
		Assert.False(reader.Read());
	}

	private static async Task WaitForConditionAsync<T>(T expected, Func<T> getValue)
	{
		var sw = Stopwatch.StartNew();
		while (sw.ElapsedMilliseconds < 1000 && !expected.Equals(getValue()))
			await Task.Delay(50);
		Assert.Equal(expected, getValue());
	}

	readonly FakeMySqlServer m_server;
	readonly MySqlConnectionStringBuilder m_csb;
}
