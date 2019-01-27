using System;
using System.Text;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Protocol.Payloads
{
	internal sealed class InitialHandshakePayload
	{
		public ProtocolCapabilities ProtocolCapabilities { get; }
		public byte[] ServerVersion { get; }
		public int ConnectionId { get; }
		public byte[] AuthPluginData { get; }
		public string? AuthPluginName { get; }

		public static InitialHandshakePayload Create(ReadOnlySpan<byte> span)
		{
			var reader = new ByteArrayReader(span);
			reader.ReadByte(c_protocolVersion);
			var serverVersion = reader.ReadNullTerminatedByteString();
			var connectionId = reader.ReadInt32();
			byte[]? authPluginData = null;
			var authPluginData1 = reader.ReadByteString(8);
			string? authPluginName = null;
			reader.ReadByte(0);
			var protocolCapabilities = (ProtocolCapabilities) reader.ReadUInt16();
			if (reader.BytesRemaining > 0)
			{
				var charSet = (CharacterSet) reader.ReadByte();
				var status = (ServerStatus) reader.ReadInt16();
				var capabilityFlagsHigh = reader.ReadUInt16();
				protocolCapabilities |= (ProtocolCapabilities) (capabilityFlagsHigh << 16);
				var authPluginDataLength = reader.ReadByte();
				var unused = reader.ReadByteString(10);
				if ((protocolCapabilities & ProtocolCapabilities.SecureConnection) != 0)
				{
					var authPluginData2 = reader.ReadByteString(Math.Max(13, authPluginDataLength - 8));
					authPluginData = new byte[authPluginData1.Length + authPluginData2.Length];
					authPluginData1.CopyTo(authPluginData);
					authPluginData2.CopyTo(new Span<byte>(authPluginData).Slice(authPluginData1.Length));
				}
				if ((protocolCapabilities & ProtocolCapabilities.PluginAuth) != 0)
					authPluginName = Encoding.UTF8.GetString(reader.ReadNullOrEofTerminatedByteString());
			}
			if (authPluginData == null)
				authPluginData = authPluginData1.ToArray();

			if (reader.BytesRemaining != 0)
				throw new FormatException("Extra bytes at end of payload.");

			return new InitialHandshakePayload(protocolCapabilities, serverVersion.ToArray(), connectionId, authPluginData, authPluginName);
		}

		private InitialHandshakePayload(ProtocolCapabilities protocolCapabilities, byte[] serverVersion, int connectionId, byte[] authPluginData, string? authPluginName)
		{
			ProtocolCapabilities = protocolCapabilities;
			ServerVersion = serverVersion;
			ConnectionId = connectionId;
			AuthPluginData = authPluginData;
			AuthPluginName = authPluginName;
		}

		const byte c_protocolVersion = 0x0A;
	}
}
