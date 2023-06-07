#nullable enable

using System.Diagnostics.Metrics;

namespace MySqlConnector.Tests.Metrics;

public abstract class MetricsTestsBase : IDisposable
{
	public MetricsTestsBase()
	{
		m_measurements = new();

		m_server = new FakeMySqlServer();
		m_server.Start();

		m_meterListener = new MeterListener
		{
			InstrumentPublished = (instrument, listener) =>
			{
				if (instrument.Meter.Name == "MySqlConnector")
					listener.EnableMeasurementEvents(instrument);
			}
		};
		m_meterListener.SetMeasurementEventCallback<int>(OnMeasurementRecorded);
		m_meterListener.Start();
	}

	public void Dispose()
	{
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}

	protected virtual void Dispose(bool disposing)
	{
		if (disposing)
		{
			m_meterListener.Dispose();
			m_server.Stop();
		}
	}

	protected string? PoolName { get; set; }
	protected int ServerActiveConnections => m_server.ActiveConnections;

	protected MySqlConnectionStringBuilder CreateConnectionStringBuilder() =>
		new MySqlConnectionStringBuilder
		{
			Server = "localhost",
			Port = (uint) m_server.Port,
			UserID = "test",
			Password = "test",
		};

	protected void AssertMeasurement(string name, int expected)
	{
		lock (m_measurements)
			Assert.Equal(expected, m_measurements.GetValueOrDefault(name));
	}

	private void OnMeasurementRecorded(Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
	{
		var poolName = "";
		var stateTag = "";
		foreach (var tag in tags)
		{
			if (tag.Key == "pool.name" && tag.Value is string s1)
				poolName = s1;
			else if (tag.Key == "state" && tag.Value is string s2)
				stateTag = s2;
		}
		if (poolName != PoolName)
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
}
