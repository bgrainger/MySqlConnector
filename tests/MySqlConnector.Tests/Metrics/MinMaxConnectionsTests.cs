#nullable enable

namespace MySqlConnector.Tests.Metrics;

public class MinMaxConnectionsTests : MetricsTestsBase
{
	[Fact]
	public void SetsMinimumIdleToDefault()
	{
		var csb = CreateConnectionStringBuilder();

		PoolName = "min-idle";
		using var dataSource = new MySqlDataSourceBuilder(csb.ConnectionString)
			.UseName(PoolName)
			.Build();
		using var connection = dataSource.OpenConnection();

		AssertMeasurement("db.client.connections.idle.min", 0);
	}

	[Fact]
	public void SetsMaximumIdleToDefault()
	{
		var csb = CreateConnectionStringBuilder();

		PoolName = "max-idle";
		using var dataSource = new MySqlDataSourceBuilder(csb.ConnectionString)
			.UseName(PoolName)
			.Build();
		using var connection = dataSource.OpenConnection();

		AssertMeasurement("db.client.connections.idle.max", 100);
	}

	[Fact]
	public void SetsMaximumToDefault()
	{
		var csb = CreateConnectionStringBuilder();

		PoolName = "max";
		using var dataSource = new MySqlDataSourceBuilder(csb.ConnectionString)
			.UseName(PoolName)
			.Build();
		using var connection = dataSource.OpenConnection();

		AssertMeasurement("db.client.connections.max", 100);
	}

	[Fact]
	public void SetsMinimumIdleToCustom()
	{
		var csb = CreateConnectionStringBuilder();
		csb.MinimumPoolSize = 3;

		PoolName = "min-idle";
		using (var dataSource = new MySqlDataSourceBuilder(csb.ConnectionString)
			.UseName(PoolName)
			.Build())
		{
			using var connection = dataSource.OpenConnection();

			AssertMeasurement("db.client.connections.idle.min", 3);
		}

		AssertMeasurement("db.client.connections.idle.min", 0);
	}

	[Fact]
	public void SetsMaximumIdleToCustom()
	{
		var csb = CreateConnectionStringBuilder();
		csb.MaximumPoolSize = 57;

		PoolName = "max-idle";
		using (var dataSource = new MySqlDataSourceBuilder(csb.ConnectionString)
			.UseName(PoolName)
			.Build())
		{
			using var connection = dataSource.OpenConnection();

			AssertMeasurement("db.client.connections.idle.max", 57);
		}

		AssertMeasurement("db.client.connections.idle.max", 0);
	}

	[Fact]
	public void SetsMaximumToCustom()
	{
		var csb = CreateConnectionStringBuilder();
		csb.MaximumPoolSize = 99;

		PoolName = "max";
		using (var dataSource = new MySqlDataSourceBuilder(csb.ConnectionString)
			.UseName(PoolName)
			.Build())
		{
			using var connection = dataSource.OpenConnection();

			AssertMeasurement("db.client.connections.max", 99);
		}

		AssertMeasurement("db.client.connections.max", 0);
	}
}
