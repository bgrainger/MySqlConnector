using System;
using System.Text;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Protocol.Payloads
{
	internal sealed class OkPayload
	{
		public int AffectedRowCount { get; }
		public ulong LastInsertId { get; }
		public ServerStatus ServerStatus { get; }
		public int WarningCount { get; }
		public string? NewSchema { get; }

		public const byte Signature = 0x00;

		/* See
		 * https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
		 * https://mariadb.com/kb/en/the-mariadb-library/resultset/
		 * https://github.com/MariaDB/mariadb-connector-j/blob/5fa814ac6e1b4c9cb6d141bd221cbd5fc45c8a78/src/main/java/org/mariadb/jdbc/internal/com/read/resultset/SelectResultSet.java#L443-L444
		 */
		public static bool IsOk(ReadOnlySpan<byte> span, bool deprecateEof) =>
			span.Length > 0 &&
				(span.Length > 6 && span[0] == Signature ||
				 deprecateEof && span.Length < 0xFF_FFFF && span[0] == EofPayload.Signature);

		public static OkPayload Create(ReadOnlySpan<byte> span, bool deprecateEof, bool clientSessionTrack)
		{
			var reader = new ByteArrayReader(span);
			var signature = reader.ReadByte();
			if (signature != Signature && (!deprecateEof || signature != EofPayload.Signature))
				throw new FormatException("Expected to read 0x00 or 0xFE but got 0x{0:X2}".FormatInvariant(signature));
			var affectedRowCount = checked((int) reader.ReadLengthEncodedInteger());
			var lastInsertId = reader.ReadLengthEncodedInteger();
			var serverStatus = (ServerStatus) reader.ReadUInt16();
			var warningCount = (int) reader.ReadUInt16();
			string? newSchema = null;

			if (clientSessionTrack && (serverStatus & ServerStatus.SessionStateChanged) == ServerStatus.SessionStateChanged)
			{
				reader.ReadLengthEncodedByteString(); // human-readable info

				// implies ProtocolCapabilities.SessionTrack
				var sessionStateChangeDataLength = checked((int) reader.ReadLengthEncodedInteger());
				var endOffset = reader.Offset + sessionStateChangeDataLength;
				while (reader.Offset < endOffset)
				{
					var kind = (SessionTrackKind) reader.ReadByte();
					var dataLength = (int) reader.ReadLengthEncodedInteger();
					switch (kind)
					{
					case SessionTrackKind.Schema:
						newSchema = Encoding.UTF8.GetString(reader.ReadLengthEncodedByteString());
						break;

					default:
						reader.Offset += dataLength;
						break;
					}
				}
			}
			else
			{
				// either "string<EOF> info" or "string<lenenc> info" (followed by no session change info)
				// ignore human-readable string in both cases
			}

			if (affectedRowCount == 0 && lastInsertId == 0 && warningCount == 0 && newSchema == null)
			{
				if (serverStatus == ServerStatus.AutoCommit)
					return s_autoCommitOk;
				if (serverStatus == (ServerStatus.AutoCommit | ServerStatus.SessionStateChanged))
					return s_autoCommitSessionStateChangedOk;
			}

			return new OkPayload(affectedRowCount, lastInsertId, serverStatus, warningCount, newSchema);
		}

		private OkPayload(int affectedRowCount, ulong lastInsertId, ServerStatus serverStatus, int warningCount, string? newSchema)
		{
			AffectedRowCount = affectedRowCount;
			LastInsertId = lastInsertId;
			ServerStatus = serverStatus;
			WarningCount = warningCount;
			NewSchema = newSchema;
		}

		static readonly OkPayload s_autoCommitOk = new OkPayload(0, 0, ServerStatus.AutoCommit, 0, null);
		static readonly OkPayload s_autoCommitSessionStateChangedOk = new OkPayload(0, 0, ServerStatus.AutoCommit | ServerStatus.SessionStateChanged, 0, null);
	}
}
