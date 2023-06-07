#nullable enable

namespace MySqlConnector.Tests.Metrics;

public class ConnectionsUsageTests : MetricsTestsBase
{
    [Fact]
	public void NamedDataSource()
    {
		PoolName = "metrics-test";
		using var dataSource = new MySqlDataSourceBuilder(CreateConnectionStringBuilder().ConnectionString)
			.UseName(PoolName)
			.Build();

		// no connections at beginning of test
		AssertMeasurement("db.client.connections.usage", 0);
		AssertMeasurement("db.client.connections.usage|idle", 0);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(0, ServerActiveConnections);

		// opening a connection creates a 'used' connection
		using (var connection = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
			Assert.Equal(1, ServerActiveConnections);
		}

		// closing it creates an 'idle' connection
		AssertMeasurement("db.client.connections.usage", 1);
		AssertMeasurement("db.client.connections.usage|idle", 1);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(1, ServerActiveConnections);

		// reopening the connection transitions it back to 'used'
		using (var connection = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
		}
		Assert.Equal(1, ServerActiveConnections);

		// opening a second connection creates a net new 'used' connection
		using (var connection = dataSource.OpenConnection())
		using (var connection2 = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 2);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 2);
			Assert.Equal(2, ServerActiveConnections);
		}

		AssertMeasurement("db.client.connections.usage", 2);
		AssertMeasurement("db.client.connections.usage|idle", 2);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(2, ServerActiveConnections);
	}

	[Fact]
	public void NamedDataSourceWithMinPoolSize()
	{
		var csb = CreateConnectionStringBuilder();
		csb.MinimumPoolSize = 3;

		PoolName = "minimum-pool-size";
		using var dataSource = new MySqlDataSourceBuilder(csb.ConnectionString)
			.UseName(PoolName)
			.Build();

		// minimum pool size is created lazily when the first connection is opened
		AssertMeasurement("db.client.connections.usage", 0);
		AssertMeasurement("db.client.connections.usage|idle", 0);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(0, ServerActiveConnections);

		// opening a connection creates the minimum connections then takes an idle one from the pool
		using (var connection = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 3);
			AssertMeasurement("db.client.connections.usage|idle", 2);
			AssertMeasurement("db.client.connections.usage|used", 1);
			Assert.Equal(3, ServerActiveConnections);
		}

		// closing puts it back to idle
		AssertMeasurement("db.client.connections.usage", 3);
		AssertMeasurement("db.client.connections.usage|idle", 3);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(3, ServerActiveConnections);
	}

	[Fact]
	public void UnnamedDataSource()
	{
		var csb = CreateConnectionStringBuilder();

		// NOTE: pool "name" is connection string (without password)
		PoolName = csb.GetConnectionString(includePassword: false);

		using var dataSource = new MySqlDataSourceBuilder(csb.ConnectionString)
			.Build();

		// no connections at beginning of test
		AssertMeasurement("db.client.connections.usage", 0);
		AssertMeasurement("db.client.connections.usage|idle", 0);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(0, ServerActiveConnections);

		// opening a connection creates a 'used' connection
		using (var connection = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
			Assert.Equal(1, ServerActiveConnections);
		}

		// closing it creates an 'idle' connection
		AssertMeasurement("db.client.connections.usage", 1);
		AssertMeasurement("db.client.connections.usage|idle", 1);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(1, ServerActiveConnections);

		// reopening the connection transitions it back to 'used'
		using (var connection = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
		}
		Assert.Equal(1, ServerActiveConnections);

		// opening a second connection creates a net new 'used' connection
		using (var connection = dataSource.OpenConnection())
		using (var connection2 = dataSource.OpenConnection())
		{
			AssertMeasurement("db.client.connections.usage", 2);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 2);
			Assert.Equal(2, ServerActiveConnections);
		}

		AssertMeasurement("db.client.connections.usage", 2);
		AssertMeasurement("db.client.connections.usage|idle", 2);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(2, ServerActiveConnections);
	}

	[Fact]
	public void NoDataSource()
	{
		var csb = CreateConnectionStringBuilder();

		// NOTE: pool "name" is connection string (without password)
		PoolName = csb.GetConnectionString(includePassword: false);

		// no connections at beginning of test
		AssertMeasurement("db.client.connections.usage", 0);
		AssertMeasurement("db.client.connections.usage|idle", 0);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(0, ServerActiveConnections);

		// opening a connection creates a 'used' connection
		using (var connection = new MySqlConnection(csb.ConnectionString))
		{
			connection.Open();
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
			Assert.Equal(1, ServerActiveConnections);
		}

		// closing it creates an 'idle' connection
		AssertMeasurement("db.client.connections.usage", 1);
		AssertMeasurement("db.client.connections.usage|idle", 1);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(1, ServerActiveConnections);

		// reopening the connection transitions it back to 'used'
		using (var connection = new MySqlConnection(csb.ConnectionString))
		{
			connection.Open();
			AssertMeasurement("db.client.connections.usage", 1);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 1);
		}
		Assert.Equal(1, ServerActiveConnections);

		// opening a second connection creates a net new 'used' connection
		using (var connection = new MySqlConnection(csb.ConnectionString))
		using (var connection2 = new MySqlConnection(csb.ConnectionString))
		{
			connection.Open();
			connection2.Open();
			AssertMeasurement("db.client.connections.usage", 2);
			AssertMeasurement("db.client.connections.usage|idle", 0);
			AssertMeasurement("db.client.connections.usage|used", 2);
			Assert.Equal(2, ServerActiveConnections);
		}

		AssertMeasurement("db.client.connections.usage", 2);
		AssertMeasurement("db.client.connections.usage|idle", 2);
		AssertMeasurement("db.client.connections.usage|used", 0);
		Assert.Equal(2, ServerActiveConnections);
	}
}
