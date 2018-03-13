using System;
using System.Buffers.Text;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Core;
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Payloads;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Direct
{
	public sealed class MySqlSession
	{
		readonly string m_host;
		readonly int m_port;
		readonly string m_userName;
		readonly string m_password;
		readonly string m_database;
		readonly byte[] m_buffer;
		Socket m_socket;
		SocketByteHandler m_byteHandler;
		MySqlColumn[] m_columns;
		ArraySegment<byte> m_remainingData;
		ByteArrayReader m_rowReader;

		public MySqlSession(string host, int port, string userName, string password, string database)
		{
			m_host = host;
			m_port = port;
			m_userName = userName;
			m_password = password;
			m_database = database;
			m_buffer = new byte[8192];
			m_columns = new MySqlColumn[0];
		}

		public async Task<bool> ConnectAsync()
		{
			m_socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
#if NET45 || NET46
			m_socket.Connect(m_host, m_port);
#else
			await m_socket.ConnectAsync(m_host, m_port);
#endif

			m_byteHandler = new SocketByteHandler(m_socket);
			var bytesRead = await m_byteHandler.ReadBytesAsync(new ArraySegment<byte>(m_buffer), IOBehavior.Asynchronous).ConfigureAwait(false);
			var payload = GetPayload(new ArraySegment<byte>(m_buffer, 0, bytesRead));
			var initialHandshake = InitialHandshakePayload.Create(payload);

			payload = HandshakeResponse41Payload.Create(initialHandshake, m_userName, m_password, m_database, true, false, null);
			payload.ArraySegment.Array[payload.ArraySegment.Offset + 2] ^= 0x10; // HACK: flip ConnectionAttributes bit
			await SendAsync(1, payload).ConfigureAwait(false);
			bytesRead = await m_byteHandler.ReadBytesAsync(new ArraySegment<byte>(m_buffer), IOBehavior.Asynchronous).ConfigureAwait(false);
			payload = GetPayload(new ArraySegment<byte>(m_buffer, 0, bytesRead));
			if (payload.HeaderByte != 0)
				throw new InvalidOperationException("couldn't log in");
			return true;
		}

		public bool Ping()
		{
			Send(0, PingPayload.Instance);
			var bytesRead = m_byteHandler.ReadBytesAsync(new ArraySegment<byte>(m_buffer), IOBehavior.Synchronous).Result;
			var payload = GetPayload(new ArraySegment<byte>(m_buffer, 0, bytesRead));
			return payload.HeaderByte == 0;
		}

		public async ValueTask<bool> PingAsync()
		{
			await SendAsync(0, PingPayload.Instance).ConfigureAwait(false);
			var bytesRead = await m_byteHandler.ReadBytesAsync(new ArraySegment<byte>(m_buffer), IOBehavior.Asynchronous).ConfigureAwait(false);
			var payload = GetPayload(new ArraySegment<byte>(m_buffer, 0, bytesRead));
			return payload.HeaderByte == 0;
		}

		public async ValueTask<MySqlColumn[]> ExecuteAsync(string sql)
		{
			var length = Encoding.UTF8.GetBytes(sql, 0, sql.Length, m_buffer, 5) + 1;
			m_buffer[0] = (byte) length;
			m_buffer[1] = (byte) (length >> 8);
			m_buffer[2] = (byte) (length >> 16);
			m_buffer[3] = 0;
			m_buffer[4] = (byte) CommandKind.Query;
			await m_byteHandler.WriteBytesAsync(new ArraySegment<byte>(m_buffer, 0, length + 4), IOBehavior.Asynchronous).ConfigureAwait(false);

			var bytesRead = await m_byteHandler.ReadBytesAsync(new ArraySegment<byte>(m_buffer), IOBehavior.Asynchronous).ConfigureAwait(false);
			m_remainingData = new ArraySegment<byte>(m_buffer, 0, bytesRead);
			var payload = GetPayload(m_remainingData);
			m_remainingData = m_remainingData.Slice(payload.ArraySegment.Count + 4);

			switch (payload.HeaderByte)
			{
			case 0:
				return new MySqlColumn[0];
			case 0xFB:
				throw new NotSupportedException("LOAD DATA LOCAL INFILE not supported.");
			case 0xFF:
				throw ErrorPayload.Create(payload).ToException();
			}

			var byteArrayReader = new ByteArrayReader(payload.ArraySegment);
			Array.Resize(ref m_columns, (int) byteArrayReader.ReadLengthEncodedInteger());
			for (int i = 0; i < m_columns.Length; i++)
			{
				payload = GetPayload(m_remainingData);
				m_remainingData = m_remainingData.Slice(payload.ArraySegment.Count + 4);
				var columnDefinition = ColumnDefinitionPayload.Create(payload.ArraySegment);
				m_columns[i] = new MySqlColumn(columnDefinition.Name, TypeMapper.ConvertToMySqlDbType(columnDefinition, true, false));
			}

			return m_columns;
		}

		public ValueTask<bool> ReadAsync()
		{
			var payload = GetPayload(m_remainingData);
			m_remainingData = m_remainingData.Slice(payload.ArraySegment.Count + 4);
			if (OkPayload.IsOk(payload, true))
				return new ValueTask<bool>(false);
			m_rowReader = new ByteArrayReader(payload.ArraySegment);
			return new ValueTask<bool>(true);
		}

		public int ReadInt32()
		{
			var columnLength = (int) m_rowReader.ReadLengthEncodedInteger(); // ASSUME: not DBNull
			if (!Utf8Parser.TryParse(new ReadOnlySpan<byte>(m_buffer, m_rowReader.Offset, columnLength), out int value, out var bytesConsumed) || columnLength != bytesConsumed)
				throw new FormatException("Couldn't parse as int");
			m_rowReader.ReadByteArraySegment(columnLength);
			return value;
		}

		public string ReadString()
		{
			var columnLength = (int) m_rowReader.ReadLengthEncodedInteger(); // ASSUME: not DBNull
			var utf8Bytes = m_rowReader.ReadByteArraySegment(columnLength);
			return Encoding.UTF8.GetString(utf8Bytes);
		}

		private int Send(int packetNumber, PayloadData payload)
		{
			var length = payload.ArraySegment.Count;
			m_buffer[0] = (byte) length;
			m_buffer[1] = (byte) (length >> 8);
			m_buffer[2] = (byte) (length >> 16);
			m_buffer[3] = (byte) packetNumber;
			Buffer.BlockCopy(payload.ArraySegment.Array, payload.ArraySegment.Offset, m_buffer, 4, length);
			return m_byteHandler.WriteBytesAsync(new ArraySegment<byte>(m_buffer, 0, length + 4), IOBehavior.Synchronous).Result;
		}

		private ValueTask<int> SendAsync(int packetNumber, PayloadData payload)
		{
			var length = payload.ArraySegment.Count;
			m_buffer[0] = (byte) length;
			m_buffer[1] = (byte) (length >> 8);
			m_buffer[2] = (byte) (length >> 16);
			m_buffer[3] = (byte) packetNumber;
			Buffer.BlockCopy(payload.ArraySegment.Array, payload.ArraySegment.Offset, m_buffer, 4, length);
			return m_byteHandler.WriteBytesAsync(new ArraySegment<byte>(m_buffer, 0, length + 4), IOBehavior.Asynchronous);
		}

		private static PayloadData GetPayload(ArraySegment<byte> packet)
		{
			var array = packet.Array;
			var length = array[packet.Offset + 0] + (array[packet.Offset + 1] * 256) + (array[packet.Offset + 2] * 65536);
			if (packet.Count < length + 4)
				throw new InvalidOperationException("expected whole packet to be read at once");
			return new PayloadData(packet.Slice(4, length));
		}
	}

	public sealed class MySqlColumn
	{
		public MySqlColumn(string name, MySqlDbType mySqlDbType)
		{
			Name = name;
			MySqlDbType = mySqlDbType;
		}

		public string Name { get; }
		public MySqlDbType MySqlDbType { get; }
	}
}
