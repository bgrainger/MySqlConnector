using MySqlConnector.Core;
using MySqlConnector.Protocol.Serialization;

namespace MySqlConnector.Protocol.Payloads
{
	internal sealed class HandshakeResponse41Payload
	{
		private static PayloadWriter CreateCapabilitiesPayload(ProtocolCapabilities serverCapabilities, ConnectionSettings cs, bool useCompression, ProtocolCapabilities additionalCapabilities=0)
		{
			var writer = new PayloadWriter();

			writer.WriteInt32((int) (
				ProtocolCapabilities.Protocol41 |
				ProtocolCapabilities.LongPassword |
				ProtocolCapabilities.SecureConnection |
				(serverCapabilities & ProtocolCapabilities.PluginAuth) |
				(serverCapabilities & ProtocolCapabilities.PluginAuthLengthEncodedClientData) |
				ProtocolCapabilities.MultiStatements |
				ProtocolCapabilities.MultiResults |
				ProtocolCapabilities.LocalFiles |
				(string.IsNullOrWhiteSpace(cs.Database) ? 0 : ProtocolCapabilities.ConnectWithDatabase) |
				(cs.UseAffectedRows ? 0 : ProtocolCapabilities.FoundRows) |
				(useCompression ? ProtocolCapabilities.Compress : ProtocolCapabilities.None) |
				(serverCapabilities & ProtocolCapabilities.ConnectionAttributes) |
				(serverCapabilities & ProtocolCapabilities.SessionTrack) |
				(serverCapabilities & ProtocolCapabilities.DeprecateEof) |
				(serverCapabilities & ProtocolCapabilities.OptionalResultSetMetadata) |
				additionalCapabilities));
			writer.WriteInt32(0x4000_0000);
			writer.WriteByte((byte) CharacterSet.Utf8Mb4Binary);
			writer.Write(new byte[23]);

			return writer;
		}

		public static PayloadData CreateWithSsl(ProtocolCapabilities serverCapabilities, ConnectionSettings cs, bool useCompression) =>
			CreateCapabilitiesPayload(serverCapabilities, cs, useCompression, ProtocolCapabilities.Ssl).ToPayloadData();

		public static PayloadData Create(InitialHandshakePayload handshake, ConnectionSettings cs, bool useCompression, byte[] connectionAttributes)
		{
			// TODO: verify server capabilities
			var writer = CreateCapabilitiesPayload(handshake.ProtocolCapabilities, cs, useCompression);
			writer.WriteNullTerminatedString(cs.UserID);
			var authenticationResponse = AuthenticationUtility.CreateAuthenticationResponse(handshake.AuthPluginData, 0, cs.Password);
			writer.WriteByte((byte) authenticationResponse.Length);
			writer.Write(authenticationResponse);

			if (!string.IsNullOrWhiteSpace(cs.Database))
				writer.WriteNullTerminatedString(cs.Database);

			if ((handshake.ProtocolCapabilities & ProtocolCapabilities.PluginAuth) != 0)
				writer.WriteNullTerminatedString("mysql_native_password");

			if (connectionAttributes != null)
				writer.Write(connectionAttributes);

			return writer.ToPayloadData();
		}
	}
}
