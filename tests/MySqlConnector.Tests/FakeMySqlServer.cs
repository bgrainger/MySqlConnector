using System.Net;
using System.Net.Sockets;

namespace MySqlConnector.Tests;

public sealed class FakeMySqlServer
{
	public FakeMySqlServer()
	{
		m_tcpListener = new(IPAddress.Any, 0);
		m_lock = new();
		m_connections = new();
		m_tasks = new();
	}

	public void Start()
	{
		m_activeConnections = 0;
		m_cts = new();
		m_tcpListener.Start();
		m_tasks.Add(AcceptConnectionsAsync());
	}

	public void Stop()
	{
		if (m_cts is not null)
		{
			m_cts.Cancel();
			m_tcpListener.Stop();
			try
			{
				Task.WaitAll(m_tasks.ToArray());
			}
			catch (AggregateException)
			{
			}
			m_connections.Clear();
			m_tasks.Clear();
			m_cts.Dispose();
			m_cts = null;
		}
	}

	public int Port => ((IPEndPoint) m_tcpListener.LocalEndpoint).Port;

	public int ActiveConnections => m_activeConnections;

	public string ServerVersion { get; set; } = "5.7.10-test";

	public bool SuppressAuthPluginNameTerminatingNull { get; set; }
	public bool SendIncompletePostHandshakeResponse { get; set; }
	public TimeSpan? ConnectDelay { get; set; }
	public TimeSpan? ResetDelay { get; set; }

	internal void CancelQuery(int connectionId)
	{
		lock (m_lock)
		{
			if (connectionId >= 1 && connectionId <= m_connections.Count)
				m_connections[connectionId - 1].CancelQueryEvent.Set();
		}
	}

	internal void ClientDisconnected() => Interlocked.Decrement(ref m_activeConnections);

	private async Task AcceptConnectionsAsync()
	{
		while (true)
		{
			var tcpClient = await m_tcpListener.AcceptTcpClientAsync();
			Interlocked.Increment(ref m_activeConnections);
			lock (m_lock)
			{
				var connection = new FakeMySqlServerConnection(this, m_tasks.Count);
				m_connections.Add(connection);
				m_tasks.Add(connection.RunAsync(tcpClient, m_cts.Token));
			}
		}
	}

	readonly object m_lock;
	readonly TcpListener m_tcpListener;
	readonly List<FakeMySqlServerConnection> m_connections;
	readonly List<Task> m_tasks;
	CancellationTokenSource m_cts;
	int m_activeConnections;
}
