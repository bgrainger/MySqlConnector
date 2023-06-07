using System.Diagnostics.Metrics;
using MySqlConnector.Tests;

namespace MySqlConnector.MetricsTests;

public class ConnectionsUsageTests : IDisposable
{
	public ConnectionsUsageTests()
	{
		m_measurements = new();

		m_server = new FakeMySqlServer();
		m_server.Start();

		m_meterListener = new MeterListener();
		m_meterListener.InstrumentPublished = (instrument, listener) =>
		{
			if (instrument.Meter.Name == "MySqlConnector")
				listener.EnableMeasurementEvents(instrument);
		};
		m_meterListener.SetMeasurementEventCallback<int>(OnMeasurementRecorded);
		m_meterListener.Start();
	}

	public void Dispose()
	{
		m_meterListener.Dispose();
		m_server.Stop();
	}

    [Fact]
	public void NamedDataSource()
    {
		var csb = new MySqlConnectionStringBuilder
		{
			Server = "localhost",
			Port = (uint) m_server.Port,
			UserID = "test",
			Password = "test",
		};

		m_poolName = "metrics-test";
		using var dataSource = new MySqlDataSourceBuilder(csb.ConnectionString)
			.UseName(m_poolName)
			.Build();

		// no connections at beginning of test
		AssertMeasurement("db.client.connections.usage", 0);
		AssertMeasurement("db.client.connections.usage|idle", 0);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(0, m_server.ActiveConnections);

		// opening a connection creates a 'used' connection
		using (var connection = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
			Assert.Equal(1, m_server.ActiveConnections);
		}

		// closing it creates an 'idle' connection
		AssertMeasurement("db.client.connections.usage", 1);
		AssertMeasurement("db.client.connections.usage|idle", 1);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(1, m_server.ActiveConnections);

		// reopening the connection transitions it back to 'used'
		using (var connection = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
		}
		Assert.Equal(1, m_server.ActiveConnections);

		// opening a second connection creates a net new 'used' connection
		using (var connection = dataSource.OpenConnection())
		using (var connection2 = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 2);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 2);
			Assert.Equal(2, m_server.ActiveConnections);
		}

		AssertMeasurement("db.client.connections.usage", 2);
		AssertMeasurement("db.client.connections.usage|idle", 2);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(2, m_server.ActiveConnections);
	}

	[Fact]
	public void NamedDataSourceWithMinPoolSize()
	{
		var csb = new MySqlConnectionStringBuilder
		{
			Server = "localhost",
			Port = (uint) m_server.Port,
			UserID = "test",
			Password = "test",
			MinimumPoolSize = 3,
		};

		m_poolName = "minimum-pool-size";
		using var dataSource = new MySqlDataSourceBuilder(csb.ConnectionString)
			.UseName(m_poolName)
			.Build();

		// minimum pool size is created lazily when the first connection is opened
		AssertMeasurement("db.client.connections.usage", 0);
		AssertMeasurement("db.client.connections.usage|idle", 0);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(0, m_server.ActiveConnections);

		// opening a connection creates the minimum connections then takes an idle one from the pool
		using (var connection = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 3);
			AssertMeasurement("db.client.connections.usage|idle", 2);
			AssertMeasurement("db.client.connections.usage|used", 1);
			Assert.Equal(3, m_server.ActiveConnections);
		}

		// closing puts it back to idle
		AssertMeasurement("db.client.connections.usage", 3);
		AssertMeasurement("db.client.connections.usage|idle", 3);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(3, m_server.ActiveConnections);
	}

	[Fact]
	public void UnnamedDataSource()
	{
		var csb = new MySqlConnectionStringBuilder
		{
			Server = "localhost",
			Port = (uint) m_server.Port,
			UserID = "test",
		};

		// NOTE: pool "name" is connection string (without password)
		m_poolName = csb.ConnectionString;

		csb.Password = "test";

		using var dataSource = new MySqlDataSourceBuilder(csb.ConnectionString)
			.Build();

		// no connections at beginning of test
		AssertMeasurement("db.client.connections.usage", 0);
		AssertMeasurement("db.client.connections.usage|idle", 0);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(0, m_server.ActiveConnections);

		// opening a connection creates a 'used' connection
		using (var connection = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
			Assert.Equal(1, m_server.ActiveConnections);
		}

		// closing it creates an 'idle' connection
		AssertMeasurement("db.client.connections.usage", 1);
		AssertMeasurement("db.client.connections.usage|idle", 1);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(1, m_server.ActiveConnections);

		// reopening the connection transitions it back to 'used'
		using (var connection = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
		}
		Assert.Equal(1, m_server.ActiveConnections);

		// opening a second connection creates a net new 'used' connection
		using (var connection = dataSource.OpenConnection())
		using (var connection2 = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 2);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 2);
			Assert.Equal(2, m_server.ActiveConnections);
		}

		AssertMeasurement("db.client.connections.usage", 2);
		AssertMeasurement("db.client.connections.usage|idle", 2);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(2, m_server.ActiveConnections);
	}

	[Fact]
	public void NoDataSource()
	{
		var csb = new MySqlConnectionStringBuilder
		{
			Server = "localhost",
			Port = (uint) m_server.Port,
			UserID = "test",
		};

		// NOTE: pool "name" is connection string (without password)
		m_poolName = csb.ConnectionString;

		csb.Password = "test";

		// no connections at beginning of test
		AssertMeasurement("db.client.connections.usage", 0);
		AssertMeasurement("db.client.connections.usage|idle", 0);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(0, m_server.ActiveConnections);

		// opening a connection creates a 'used' connection
		using (var connection = new MySqlConnection(csb.ConnectionString))
		{
			connection.Open();
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
			Assert.Equal(1, m_server.ActiveConnections);
		}

		// closing it creates an 'idle' connection
		AssertMeasurement("db.client.connections.usage", 1);
		AssertMeasurement("db.client.connections.usage|idle", 1);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(1, m_server.ActiveConnections);

		// reopening the connection transitions it back to 'used'
		using (var connection = new MySqlConnection(csb.ConnectionString))
		{
			connection.Open();
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
		}
		Assert.Equal(1, m_server.ActiveConnections);

		// opening a second connection creates a net new 'used' connection
		using (var connection = new MySqlConnection(csb.ConnectionString))
		using (var connection2 = new MySqlConnection(csb.ConnectionString))
		{
			connection.Open();
			connection2.Open();
			AssertMeasurement("db.client.connections.usage", 2);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 2);
			Assert.Equal(2, m_server.ActiveConnections);
		}

		AssertMeasurement("db.client.connections.usage", 2);
		AssertMeasurement("db.client.connections.usage|idle", 2);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(2, m_server.ActiveConnections);
	}

	private void AssertMeasurement(string name, int expected)
	{
		lock (m_measurements)
			Assert.Equal(expected, m_measurements.GetValueOrDefault(name));
	}

	private void OnMeasurementRecorded(Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
	{
		string poolName = "";
		string stateTag = "";
		foreach (var tag in tags)
		{
			if (tag.Key == "pool.name" && tag.Value is string s1)
				poolName = s1;
			else if (tag.Key == "state" && tag.Value is string s2)
				stateTag = s2;
		}
		if (poolName != m_poolName)
			return;

		lock (m_measurements)
		{
			m_measurements[instrument.Name] = m_measurements.GetValueOrDefault(instrument.Name) + measurement;
			if (stateTag.Length != 0)
				m_measurements[$"{instrument.Name}|{stateTag}"] = m_measurements.GetValueOrDefault($"{instrument.Name}|{stateTag}") + measurement;
		}
	}

	private readonly Dictionary<string, int> m_measurements;
	private readonly FakeMySqlServer m_server;
	private readonly MeterListener m_meterListener;
	private string? m_poolName;
}
