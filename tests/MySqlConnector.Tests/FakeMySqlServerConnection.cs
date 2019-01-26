using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Protocol;

namespace MySqlConnector.Tests
{
	internal sealed class FakeMySqlServerConnection
	{
		public FakeMySqlServerConnection(FakeMySqlServer server, int connectionId)
		{
			m_server = server ?? throw new ArgumentNullException(nameof(server));
			m_connectionId = connectionId;
		}

		public async Task RunAsync(TcpClient client, CancellationToken token)
		{
			try
			{
				using (token.Register(client.Dispose))
				using (client)
				using (var stream = client.GetStream())
				{
					await SendAsync(stream, 0, WriteInitialHandshake);
					await ReadPayloadAsync(stream, token); // handshake response

					if (m_server.SendIncompletePostHandshakeResponse)
					{
						await stream.WriteAsync(new byte[] { 1, 0, 0, 2 }, 0, 4);
						return;
					}

					await SendAsync(stream, 2, WriteOk);

					var keepRunning = true;
					while (keepRunning)
					{
						byte[] bytes;
						try
						{
							bytes = await ReadPayloadAsync(stream, token);
						}
						catch (EndOfStreamException)
						{
							break;
						}

						switch ((CommandKind) bytes[0])
						{
						case CommandKind.Quit:
							await SendAsync(stream, 1, WriteOk);
							keepRunning = false;
							break;

						case CommandKind.Ping:
						case CommandKind.ResetConnection:
							await SendAsync(stream, 1, WriteOk);
							break;

						case CommandKind.Query:
							var query = Encoding.UTF8.GetString(bytes, 1, bytes.Length - 1);
							Match match;
							if (query == "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci;")
							{
								await SendAsync(stream, 1, WriteOk);
							}
							else if ((match = Regex.Match(query, @"^SELECT ([0-9]+)(;|$)")).Success)
							{
								var number = match.Groups[1].Value;
								var data = new byte[number.Length + 1];
								data[0] = (byte) number.Length;
								Encoding.UTF8.GetBytes(number, 0, number.Length, data, 1);

								await SendAsync(stream, 1, x => x.Write((byte) 1)); // one column
								await SendAsync(stream, 2, x => x.Write(new byte[] { 3, 0x64, 0x65, 0x66, 0, 0, 0, 1, 0x5F, 0, 0x0c, 0x3f, 0, 1, 0, 0, 0, 8, 0x81, 0, 0, 0, 0 })); // column definition
								await SendAsync(stream, 3, x => x.Write(new byte[] { 0xFE, 0, 0, 2, 0 })); // EOF
								await SendAsync(stream, 4, x => x.Write(data));
								await SendAsync(stream, 5, x => x.Write(new byte[] { 0xFE, 0, 0, 2, 0 })); // EOF
							}
							else
							{
								await SendAsync(stream, 1, x => WriteError(x, "Unhandled query: " + query));
							}
							break;

						default:
							Console.WriteLine("** UNHANDLED ** {0}", (CommandKind) bytes[0]);
							await SendAsync(stream, 1, x => WriteError(x));
							break;
						}
					}
				}
			}
			finally
			{
				m_server.ClientDisconnected();
			}
		}

		private static async Task SendAsync(Stream stream, int sequenceNumber, Action<BinaryWriter> writePayload)
		{
			var packet = MakePayload(sequenceNumber, writePayload);
			await stream.WriteAsync(packet, 0, packet.Length);
		}

		private static byte[] MakePayload(int sequenceNumber, Action<BinaryWriter> writePayload)
		{
			using var memoryStream = new MemoryStream();
			using (var writer = new BinaryWriter(memoryStream, Encoding.UTF8, leaveOpen: true))
			{
				writer.Write(default(int));
				writePayload(writer);
				memoryStream.Position = 0;
				writer.Write(((int) (memoryStream.Length - 4)) | ((sequenceNumber % 256) << 24));
			}
			return memoryStream.ToArray();
		}

		private static async Task<byte[]> ReadPayloadAsync(Stream stream, CancellationToken token)
		{
			var header = await ReadBytesAsync(stream, 4, token);
			var length = header[0] | (header[1] << 8) | (header[2] << 16);
			var sequenceNumber = header[3];
			return await ReadBytesAsync(stream, length, token);
		}

		private static async Task<byte[]> ReadBytesAsync(Stream stream, int count, CancellationToken token)
		{
			var bytes = new byte[count];
			for (var totalBytesRead = 0; totalBytesRead < count;)
			{
				var bytesRead = await stream.ReadAsync(bytes, totalBytesRead, count - totalBytesRead, token);
				if (bytesRead == 0)
					throw new EndOfStreamException();
				totalBytesRead += bytesRead;
			}
			return bytes;
		}

		private void WriteInitialHandshake(BinaryWriter writer)
		{
			var random = new Random(1);
			var authData = new byte[20];
			random.NextBytes(authData);
			var capabilities =
				ProtocolCapabilities.LongPassword |
				ProtocolCapabilities.FoundRows |
				ProtocolCapabilities.LongFlag |
				ProtocolCapabilities.IgnoreSpace |
				ProtocolCapabilities.Protocol41 |
				ProtocolCapabilities.Transactions |
				ProtocolCapabilities.SecureConnection |
				ProtocolCapabilities.MultiStatements |
				ProtocolCapabilities.MultiResults |
				ProtocolCapabilities.PluginAuth |
				ProtocolCapabilities.ConnectionAttributes |
				ProtocolCapabilities.PluginAuthLengthEncodedClientData;

			writer.Write((byte) 10); // protocol version
			writer.WriteNullTerminated(m_server.ServerVersion); // server version
			writer.Write(m_connectionId); // conection ID
			writer.Write(authData, 0, 8); // auth plugin data part 1
			writer.Write((byte) 0); // filler
			writer.Write((ushort) capabilities);
			writer.Write((byte) CharacterSet.Utf8Binary); // character set
			writer.Write((ushort) 0); // status flags
			writer.Write((ushort) ((uint) capabilities >> 16));
			writer.Write((byte) authData.Length);
			writer.Write(new byte[10]); // reserved
			writer.Write(authData, 8, authData.Length - 8);
			if (authData.Length - 8 < 13)
				writer.Write(new byte[13 - (authData.Length - 8)]); // have to write at least 13 bytes
			writer.Write(Encoding.UTF8.GetBytes("mysql_native_password"));
			if (!m_server.SuppressAuthPluginNameTerminatingNull)
				writer.Write((byte) 0);
		}

		private static void WriteOk(BinaryWriter writer)
		{
			writer.Write((byte) 0); // signature
			writer.Write((byte) 0); // 0 rows affected
			writer.Write((byte) 0); // last insert ID
			writer.Write((ushort) 0); // server status
			writer.Write((ushort) 0); // warning count
		}

		private static void WriteError(BinaryWriter writer, string message = "An unknown error occurred")
		{
			writer.Write((byte) 0xFF); // signature
			writer.Write((ushort) MySqlErrorCode.UnknownError); // error code
			writer.WriteRaw("#ERROR");
			writer.WriteRaw(message);
		}

		readonly FakeMySqlServer m_server;
		readonly int m_connectionId;
	}
}
