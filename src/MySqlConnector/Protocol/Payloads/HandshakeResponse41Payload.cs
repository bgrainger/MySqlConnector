using MySqlConnector.Core;
using MySqlConnector.Protocol.Serialization;

namespace MySqlConnector.Protocol.Payloads
{
	internal static class HandshakeResponse41Payload
	{
		private static ByteBufferWriter CreateCapabilitiesPayload(ProtocolCapabilities serverCapabilities, ConnectionSettings cs, bool useCompression, CharacterSet characterSet, ProtocolCapabilities additionalCapabilities = 0)
		{
			var writer = new ByteBufferWriter();

			writer.Write((int) (
				ProtocolCapabilities.Protocol41 |
				(cs.InteractiveSession ? (serverCapabilities & ProtocolCapabilities.Interactive) : 0) |
				ProtocolCapabilities.LongPassword |
				(serverCapabilities & ProtocolCapabilities.Transactions) |
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
				additionalCapabilities));
			writer.Write(0x4000_0000);
			writer.Write((byte) characterSet);
			writer.Write(s_padding);

			return writer;
		}

		public static PayloadData CreateWithSsl(ProtocolCapabilities serverCapabilities, ConnectionSettings cs, bool useCompression, CharacterSet characterSet) =>
			CreateCapabilitiesPayload(serverCapabilities, cs, useCompression, characterSet, ProtocolCapabilities.Ssl).ToPayloadData();

		public static PayloadData Create(InitialHandshakePayload handshake, ConnectionSettings cs, bool useCompression, CharacterSet characterSet, byte[]? connectionAttributes)
		{
			// TODO: verify server capabilities
			var writer = CreateCapabilitiesPayload(handshake.ProtocolCapabilities, cs, useCompression, characterSet);
			writer.WriteNullTerminatedString(cs.UserID);
			var authenticationResponse = AuthenticationUtility.CreateAuthenticationResponse(handshake.AuthPluginData, 0, cs.Password);
			writer.Write((byte) authenticationResponse.Length);
			writer.Write(authenticationResponse);

			if (!string.IsNullOrWhiteSpace(cs.Database))
				writer.WriteNullTerminatedString(cs.Database);

			if ((handshake.ProtocolCapabilities & ProtocolCapabilities.PluginAuth) != 0)
				writer.WriteNullTerminatedString("mysql_native_password");

			if (connectionAttributes != null)
				writer.Write(connectionAttributes);

			return writer.ToPayloadData();
		}

		static readonly byte[] s_padding = new byte[23];
	}
}
