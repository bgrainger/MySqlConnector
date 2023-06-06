using System.Diagnostics.Metrics;
using MySqlConnector.Tests;

namespace MySqlConnector.MetricsTests;

public class Tests : IDisposable
{
	public Tests()
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
    public void TestMetrics()
    {
		var csb = new MySqlConnectionStringBuilder
		{
			Server = "localhost",
			Port = (uint) m_server.Port,
			UserID = "test",
			Password = "test",
		};

		// no connections at beginning of test
		AssertMeasurement("hard-connects", 0);
		AssertMeasurement("soft-connects", 0);
		AssertMeasurement("hard-disconnects", 0);
		AssertMeasurement("soft-disconnects", 0);
		Assert.Equal(0, m_server.ActiveConnections);

		// opening a connection creates one hard connection and one soft disconnection
		using (var connection = new MySqlConnection(csb.ConnectionString))
		{
			connection.Open();
		}
		AssertMeasurement("hard-connects", 1);
		AssertMeasurement("soft-connects", 0);
		AssertMeasurement("hard-disconnects", 0);
		AssertMeasurement("soft-disconnects", 1);
		Assert.Equal(1, m_server.ActiveConnections);
		m_measurements.Clear();

		// reopening the connection creates a soft connection and disconnection
		using (var connection = new MySqlConnection(csb.ConnectionString))
		{
			connection.Open();
		}
		AssertMeasurement("hard-connects", 0);
		AssertMeasurement("soft-connects", 1);
		AssertMeasurement("hard-disconnects", 0);
		AssertMeasurement("soft-disconnects", 1);
		Assert.Equal(1, m_server.ActiveConnections);
		m_measurements.Clear();

		// opening a second connection creates a hard connection and soft disconnection
		using (var connection = new MySqlConnection(csb.ConnectionString))
		using (var connection2 = new MySqlConnection(csb.ConnectionString))
		{
			connection.Open();
			connection2.Open();
			Assert.Equal(2, m_server.ActiveConnections);
		}
		AssertMeasurement("hard-connects", 1);
		AssertMeasurement("soft-connects", 1);
		AssertMeasurement("hard-disconnects", 0);
		AssertMeasurement("soft-disconnects", 2);
		Assert.Equal(2, m_server.ActiveConnections);
		m_measurements.Clear();

		// clearing the connection pool closes hard connections
		using (var connection = new MySqlConnection(csb.ConnectionString))
		{
			MySqlConnection.ClearPool(connection);
		}
		AssertMeasurement("hard-connects", 0);
		AssertMeasurement("soft-connects", 0);
		AssertMeasurement("hard-disconnects", 2);
		AssertMeasurement("soft-disconnects", 0);
		Assert.Equal(0, m_server.ActiveConnections);
		m_measurements.Clear();
	}

	private void AssertMeasurement(string name, int expected)
	{
		lock (m_measurements)
			Assert.Equal(expected, m_measurements.GetValueOrDefault(name));
	}

	private void OnMeasurementRecorded(Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
	{
		lock (m_measurements)
			m_measurements[instrument.Name] = m_measurements.GetValueOrDefault(instrument.Name) + measurement;
	}

	private readonly Dictionary<string, int> m_measurements;
	private readonly FakeMySqlServer m_server;
	private readonly MeterListener m_meterListener;
}
