using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using MySqlConnector.Protocol;

namespace MySqlConnector.Tests;

internal sealed class FakeMySqlServerConnection
{
	public FakeMySqlServerConnection(FakeMySqlServer server, int connectionId)
	{
		m_server = server ?? throw new ArgumentNullException(nameof(server));
		m_connectionId = connectionId;
		CancelQueryEvent = new();
	}

	public ManualResetEventSlim CancelQueryEvent { get; }

	public async Task RunAsync(TcpClient client, CancellationToken token)
	{
		try
		{
			using (token.Register(client.Dispose))
			using (client)
			using (var stream = client.GetStream())
			{
				if (m_server.ConnectDelay is { } connectDelay)
					await Task.Delay(connectDelay);

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
						await SendAsync(stream, 1, WriteOk);
						break;

					case CommandKind.ResetConnection:
						if (m_server.ResetDelay is { } resetDelay)
							await Task.Delay(resetDelay);
						await SendAsync(stream, 1, WriteOk);
						break;

					case CommandKind.Query:
						var query = Encoding.UTF8.GetString(bytes, 1, bytes.Length - 1);
						Match match;
						if (query == "SET NAMES utf8mb4;")
						{
							await SendAsync(stream, 1, WriteOk);
						}
						else if ((match = Regex.Match(query, @"^SELECT ([0-9])(;|$)")).Success)
						{
							var number = match.Groups[1].Value;
							var data = new byte[number.Length + 1];
							data[0] = (byte) number.Length;
							Encoding.UTF8.GetBytes(number, 0, number.Length, data, 1);

							await SendAsync(stream, 1, x => x.Write((byte) 1)); // one column
							await SendAsync(stream, 2, x => x.Write(new byte[] { 3, 0x64, 0x65, 0x66, 0, 0, 0, 1, 0x5F, 0, 0x0c, 0x3f, 0, 1, 0, 0, 0, 3, 0x81, 0, 0, 0, 0 })); // column definition
							await SendAsync(stream, 3, x => x.Write(new byte[] { 0xFE, 0, 0, 2, 0 })); // EOF
							await SendAsync(stream, 4, x => x.Write(data));
							await SendAsync(stream, 5, x => x.Write(new byte[] { 0xFE, 0, 0, 2, 0 })); // EOF
						}
						else if ((match = Regex.Match(query, @"^SELECT ([0-9]+), ([0-9]+), ([0-9-]+), ([0-9]+)(;|$)")).Success)
						{
							// command is "SELECT {value}, {delay}, {pauseStep}, {flags}"
							var number = match.Groups[1].Value;
							var value = int.Parse(number);
							var delay = int.Parse(match.Groups[2].Value);
							var pauseStep = int.Parse(match.Groups[3].Value);
							var flags = int.Parse(match.Groups[4].Value);
							var ignoreCancellation = (flags & 1) == 1;
							var bufferOutput = (flags & 2) == 2;

							var data = new byte[number.Length + 1];
							data[0] = (byte) number.Length;
							Encoding.UTF8.GetBytes(number, 0, number.Length, data, 1);
							
							var negativeOne = new byte[] { 2, 0x2D, 0x31 };
							var packets = new[]
							{
								new byte[] { 0xFF, 0x25, 0x05, 0x23, 0x37, 0x30, 0x31, 0x30, 0x30 }.Concat(Encoding.ASCII.GetBytes("Query execution was interrupted")).ToArray(), // error
								new byte[] { 1 }, // one column
								new byte[] { 3, 0x64, 0x65, 0x66, 0, 0, 0, 1, 0x5F, 0, 0x0c, 0x3f, 0, 1, 0, 0, 0, 3, 0x81, 0, 0, 0, 0 }, // column definition
								new byte[] { 0xFE, 0, 0, 2, 0 }, // EOF
								data,
								negativeOne,
								negativeOne,
								new byte[] { 0xFE, 0, 0, 10, 0 }, // EOF, more results exist
								new byte[] { 1 }, // one column
								new byte[] { 3, 0x64, 0x65, 0x66, 0, 0, 0, 1, 0x5F, 0, 0x0c, 0x3f, 0, 1, 0, 0, 0, 3, 0x81, 0, 0, 0, 0 }, // column definition
								new byte[] { 0xFE, 0, 0, 2, 0 }, // EOF
								negativeOne,
								new byte[] { 0xFE, 0, 0, 2, 0 }, // EOF
							};

							if (bufferOutput)
							{
								// if 'bufferOutput' is set, perform the delay immediately then send all the output afterwards, as though it were buffered on the server
								var queryInterrupted = false;
								if (ignoreCancellation)
									await Task.Delay(delay, token);
								else
									queryInterrupted = CancelQueryEvent.Wait(delay, token);

								for (var step = 1; step < pauseStep; step++)
									await SendAsync(stream, step, x => x.Write(packets[step]));
								await SendAsync(stream, pauseStep, x => x.Write(packets[queryInterrupted ? 0 : pauseStep]));
							}
							else
							{
								var queryInterrupted = false;
								for (var step = 1; step < packets.Length && !queryInterrupted; step++)
								{
									if (pauseStep == step || pauseStep == -1)
									{
										if (ignoreCancellation)
											await Task.Delay(delay, token);
										else
											queryInterrupted = CancelQueryEvent.Wait(delay, token);
									}

									await SendAsync(stream, step, x => x.Write(packets[queryInterrupted ? 0 : step]));
								}
							}
						}
						else if ((match = Regex.Match(query, @"^KILL QUERY ([0-9]+)(;|$)", RegexOptions.IgnoreCase)).Success)
						{
							var connectionId = int.Parse(match.Groups[1].Value);
							m_server.CancelQuery(connectionId);
							await SendAsync(stream, 1, WriteOk);
						}
						else if (query == "SELECT SLEEP(0) INTO @\uE001MySqlConnector\uE001Sleep;")
						{
							var wasSet = CancelQueryEvent.Wait(0, token);
							await SendAsync(stream, 1, WriteOk);
						}
						else if (query == "select infinity")
						{
							var packets = new[]
							{
								new byte[] { 2 }, // two columns
								new byte[] { 3, 0x64, 0x65, 0x66, 0, 0, 0, 1, 0x46, 0, 0x0c, 0x3f, 0, 1, 0, 0, 0, 4, 0x01, 0, 0x1F, 0, 0 }, // column definition (float)
								new byte[] { 3, 0x64, 0x65, 0x66, 0, 0, 0, 1, 0x44, 0, 0x0c, 0x3f, 0, 1, 0, 0, 0, 5, 0x01, 0, 0x1F, 0, 0 }, // column definition (double)
								new byte[] { 0xFE, 0, 0, 2, 0 }, // EOF
								new byte[] { 3, 0x6e, 0x61, 0x6e, 3, 0x6e, 0x61, 0x6e }, // nan
								new byte[] { 3, 0x69, 0x6e, 0x66, 3, 0x69, 0x6e, 0x66 }, // inf
								new byte[] { 4, 0x2d, 0x69, 0x6e, 0x66, 4, 0x2d, 0x69, 0x6e, 0x66 }, // -inf
								new byte[] { 0xFE, 0, 0, 2, 0 }, // EOF
							};
							for (var packetIndex = 0; packetIndex < packets.Length; packetIndex++)
								await SendAsync(stream, packetIndex + 1, x => x.Write(packets[packetIndex]));
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
		writer.Write((byte) CharacterSet.Utf8Mb3Binary); // character set
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
