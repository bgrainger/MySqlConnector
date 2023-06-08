namespace MySqlConnector.Tests.Metrics;

public class ConnectionTimeTests : MetricsTestsBase
{
	[Fact]
	public async Task ConnectionTime()
	{
		var csb = CreateConnectionStringBuilder();
		PoolName = csb.GetConnectionString(includePassword: false);
		using var connection = new MySqlConnection(csb.ConnectionString);
		await connection.OpenAsync();
		var measurements = GetAndClearMeasurements("db.client.connections.create_time");
		var time = Assert.Single(measurements);
		Assert.InRange(time, 0, 300);
	}

	[Fact]
	public async Task ConnectionTimeWithDelay()
	{
		var csb = CreateConnectionStringBuilder();
		PoolName = csb.GetConnectionString(includePassword: false);
		Server.ConnectDelay = TimeSpan.FromSeconds(1);

		using var connection = new MySqlConnection(csb.ConnectionString);
		await connection.OpenAsync();
		var measurements = GetAndClearMeasurements("db.client.connections.create_time");
		var time = Assert.Single(measurements);
		Assert.InRange(time, 1000, 1300);
	}

	[Fact]
	public async Task OpenFromPoolTime()
	{
		var csb = CreateConnectionStringBuilder();
		PoolName = csb.GetConnectionString(includePassword: false);

		using var connection = new MySqlConnection(csb.ConnectionString);
		await connection.OpenAsync();
		connection.Close();

		await connection.OpenAsync();
		var measurements = GetAndClearMeasurements("db.client.connections.wait_time");
		var time = Assert.Single(measurements);
		Assert.InRange(time, 0, 200);
	}

	[Fact]
	public async Task OpenFromPoolTimeWithDelay()
	{
		var csb = CreateConnectionStringBuilder();
		PoolName = csb.GetConnectionString(includePassword: false);
		Server.ResetDelay = TimeSpan.FromSeconds(1);

		using var connection = new MySqlConnection(csb.ConnectionString);
		await connection.OpenAsync();
		connection.Close();

		await connection.OpenAsync();
		var measurements = GetAndClearMeasurements("db.client.connections.wait_time");
		var time = Assert.Single(measurements);
		Assert.InRange(time, 1000, 1200);
	}

	[Fact]
	public async Task UseTime()
	{
		var csb = CreateConnectionStringBuilder();
		PoolName = csb.GetConnectionString(includePassword: false);

		using var connection = new MySqlConnection(csb.ConnectionString);
		await connection.OpenAsync();
		connection.Close();

		var time = Assert.Single(GetAndClearMeasurements("db.client.connections.use_time"));
		Assert.InRange(time, 0, 100);
	}

	[Fact]
	public async Task UseTimeWithDelay()
	{
		var csb = CreateConnectionStringBuilder();
		PoolName = csb.GetConnectionString(includePassword: false);

		using var connection = new MySqlConnection(csb.ConnectionString);
		await connection.OpenAsync();
		await Task.Delay(500);
		connection.Close();

		var time = Assert.Single(GetAndClearMeasurements("db.client.connections.use_time"));
		Assert.InRange(time, 500, 600);
	}
}
