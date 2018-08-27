using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;

namespace Benchmark
{
	[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
	[CategoriesColumn]
	public class ReadonlyStruct
	{
		[BenchmarkCategory("Cached"), Benchmark(Baseline = true)]
		public int BaselineCached() => OkPayloadBaseline.Create(s_payloadDataBaselineCached).WarningCount;

		[BenchmarkCategory("Cached"), Benchmark]
		public int ReadonlyCached() => OkPayloadReadOnly.Create(in s_payloadDataReadonlyCached).WarningCount;

		[BenchmarkCategory("Cached"), Benchmark]
		public int StructSpanCached() => OkPayloadStructSpan.Create(s_payloadDataReadonlyCached.ArraySegment.AsSpan()).WarningCount;

		[BenchmarkCategory("Cached"), Benchmark]
		public int ClassSpanCached() => OkPayloadClassSpan.Create(s_payloadDataReadonlyCached.ArraySegment.AsSpan()).WarningCount;

		[BenchmarkCategory("New"), Benchmark(Baseline = true)]
		public int BaselineNew() => OkPayloadBaseline.Create(s_payloadDataBaselineNew).WarningCount;

		[BenchmarkCategory("New"), Benchmark]
		public int ReadonlyNew() => OkPayloadReadOnly.Create(in s_payloadDataReadonlyNew).WarningCount;

		[BenchmarkCategory("New"), Benchmark]
		public int StructSpanNew() => OkPayloadStructSpan.Create(s_payloadDataReadonlyNew.ArraySegment.AsSpan()).WarningCount;

		[BenchmarkCategory("New"), Benchmark]
		public int ClassSpanNew() => OkPayloadClassSpan.Create(s_payloadDataReadonlyNew.ArraySegment.AsSpan()).WarningCount;

		static readonly PayloadDataBaseline s_payloadDataBaselineCached = new PayloadDataBaseline(new byte[] { 0, 0, 0, 2, 0, 0, 0, 4, 65, 66, 67, 68 });
		static readonly PayloadDataReadonly s_payloadDataReadonlyCached = new PayloadDataReadonly(new byte[] { 0, 0, 0, 2, 0, 0, 0, 4, 65, 66, 67, 68 });
		static readonly PayloadDataBaseline s_payloadDataBaselineNew = new PayloadDataBaseline(new byte[] { 0, 0, 0, 2, 0, 1, 0, 4, 65, 66, 67, 68 });
		static readonly PayloadDataReadonly s_payloadDataReadonlyNew = new PayloadDataReadonly(new byte[] { 0, 0, 0, 2, 1, 0, 0, 4, 65, 66, 67, 68 });
	}

	internal struct PayloadDataBaseline : IDisposable
	{
		public PayloadDataBaseline(byte[] data, bool isPooled = false)
		{
			ArraySegment = new ArraySegment<byte>(data);
			m_isPooled = isPooled;
		}

		public PayloadDataBaseline(ArraySegment<byte> data, bool isPooled = false)
		{
			ArraySegment = data;
			m_isPooled = isPooled;
		}

		public ArraySegment<byte> ArraySegment { get; }
		public byte HeaderByte => ArraySegment.Array[ArraySegment.Offset];

		public void Dispose()
		{
			if (m_isPooled)
				ArrayPool<byte>.Shared.Return(ArraySegment.Array);
		}

		readonly bool m_isPooled;
	}

	internal readonly struct PayloadDataReadonly : IDisposable
	{
		public PayloadDataReadonly(byte[] data, bool isPooled = false)
		{
			ArraySegment = new ArraySegment<byte>(data);
			m_isPooled = isPooled;
		}

		public PayloadDataReadonly(ArraySegment<byte> data, bool isPooled = false)
		{
			ArraySegment = data;
			m_isPooled = isPooled;
		}

		public ArraySegment<byte> ArraySegment { get; }
		public byte HeaderByte => ArraySegment.Array[ArraySegment.Offset];

		public void Dispose()
		{
			if (m_isPooled)
				ArrayPool<byte>.Shared.Return(ArraySegment.Array);
		}

		readonly bool m_isPooled;
	}

	internal sealed class OkPayloadBaseline
	{
		public int AffectedRowCount { get; }
		public ulong LastInsertId { get; }
		public ServerStatus ServerStatus { get; }
		public int WarningCount { get; }
		public string NewSchema { get; }

		public const byte Signature = 0x00;

		/* See
		 * http://web.archive.org/web/20160604101747/http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
		 * https://mariadb.com/kb/en/the-mariadb-library/resultset/
		 * https://github.com/MariaDB/mariadb-connector-j/blob/5fa814ac6e1b4c9cb6d141bd221cbd5fc45c8a78/src/main/java/org/mariadb/jdbc/internal/com/read/resultset/SelectResultSet.java#L443-L444
		 */
		public static bool IsOk(PayloadDataBaseline payload, bool deprecateEof) =>
			payload.ArraySegment.Array != null && payload.ArraySegment.Count > 0 &&
				((payload.ArraySegment.Count > 6 && payload.ArraySegment.Array[payload.ArraySegment.Offset] == Signature) ||
				(deprecateEof && payload.ArraySegment.Count < 0xFF_FFFF && payload.ArraySegment.Array[payload.ArraySegment.Offset] == EofPayload.Signature));

		public static OkPayloadBaseline Create(PayloadDataBaseline payload) => Create(payload, false);

		public static OkPayloadBaseline Create(PayloadDataBaseline payload, bool deprecateEof)
		{
			var reader = new ByteArrayReader(payload.ArraySegment);
			var signature = reader.ReadByte();
			if (signature != Signature && (!deprecateEof || signature != EofPayload.Signature))
				throw new FormatException("Expected to read 0x00 or 0xFE but got 0x{0:X2}".FormatInvariant(signature));
			var affectedRowCount = checked((int) reader.ReadLengthEncodedInteger());
			var lastInsertId = reader.ReadLengthEncodedInteger();
			var serverStatus = (ServerStatus) reader.ReadUInt16();
			var warningCount = (int) reader.ReadUInt16();
			string newSchema = null;

			if ((serverStatus & ServerStatus.SessionStateChanged) == ServerStatus.SessionStateChanged)
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

			return new OkPayloadBaseline(affectedRowCount, lastInsertId, serverStatus, warningCount, newSchema);
		}

		private OkPayloadBaseline(int affectedRowCount, ulong lastInsertId, ServerStatus serverStatus, int warningCount, string newSchema)
		{
			AffectedRowCount = affectedRowCount;
			LastInsertId = lastInsertId;
			ServerStatus = serverStatus;
			WarningCount = warningCount;
			NewSchema = newSchema;
		}

		static readonly OkPayloadBaseline s_autoCommitOk = new OkPayloadBaseline(0, 0, ServerStatus.AutoCommit, 0, null);
		static readonly OkPayloadBaseline s_autoCommitSessionStateChangedOk = new OkPayloadBaseline(0, 0, ServerStatus.AutoCommit | ServerStatus.SessionStateChanged, 0, null);
	}

	internal readonly struct OkPayloadReadOnly
	{
		public int AffectedRowCount { get; }
		public ulong LastInsertId { get; }
		public ServerStatus ServerStatus { get; }
		public int WarningCount { get; }
		public string NewSchema { get; }

		public const byte Signature = 0x00;

		/* See
		 * http://web.archive.org/web/20160604101747/http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
		 * https://mariadb.com/kb/en/the-mariadb-library/resultset/
		 * https://github.com/MariaDB/mariadb-connector-j/blob/5fa814ac6e1b4c9cb6d141bd221cbd5fc45c8a78/src/main/java/org/mariadb/jdbc/internal/com/read/resultset/SelectResultSet.java#L443-L444
		 */
		public static bool IsOk(in PayloadDataReadonly payload, bool deprecateEof) =>
			payload.ArraySegment.Array != null && payload.ArraySegment.Count > 0 &&
				((payload.ArraySegment.Count > 6 && payload.ArraySegment.Array[payload.ArraySegment.Offset] == Signature) ||
				(deprecateEof && payload.ArraySegment.Count < 0xFF_FFFF && payload.ArraySegment.Array[payload.ArraySegment.Offset] == EofPayload.Signature));

		public static OkPayloadReadOnly Create(in PayloadDataReadonly payload) => Create(payload, false);

		public static OkPayloadReadOnly Create(in PayloadDataReadonly payload, bool deprecateEof)
		{
			var reader = new ByteArrayReader(payload.ArraySegment);
			var signature = reader.ReadByte();
			if (signature != Signature && (!deprecateEof || signature != EofPayload.Signature))
				throw new FormatException("Expected to read 0x00 or 0xFE but got 0x{0:X2}".FormatInvariant(signature));
			var affectedRowCount = checked((int) reader.ReadLengthEncodedInteger());
			var lastInsertId = reader.ReadLengthEncodedInteger();
			var serverStatus = (ServerStatus) reader.ReadUInt16();
			var warningCount = (int) reader.ReadUInt16();
			string newSchema = null;

			if ((serverStatus & ServerStatus.SessionStateChanged) == ServerStatus.SessionStateChanged)
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

			return new OkPayloadReadOnly(affectedRowCount, lastInsertId, serverStatus, warningCount, newSchema);
		}

		private OkPayloadReadOnly(int affectedRowCount, ulong lastInsertId, ServerStatus serverStatus, int warningCount, string newSchema)
		{
			AffectedRowCount = affectedRowCount;
			LastInsertId = lastInsertId;
			ServerStatus = serverStatus;
			WarningCount = warningCount;
			NewSchema = newSchema;
		}

		static readonly OkPayloadReadOnly s_autoCommitOk = new OkPayloadReadOnly(0, 0, ServerStatus.AutoCommit, 0, null);
		static readonly OkPayloadReadOnly s_autoCommitSessionStateChangedOk = new OkPayloadReadOnly(0, 0, ServerStatus.AutoCommit | ServerStatus.SessionStateChanged, 0, null);
	}

	internal sealed class OkPayloadClassSpan
	{
		public int AffectedRowCount { get; }
		public ulong LastInsertId { get; }
		public ServerStatus ServerStatus { get; }
		public int WarningCount { get; }
		public string NewSchema { get; }

		public const byte Signature = 0x00;

		/* See
		 * http://web.archive.org/web/20160604101747/http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
		 * https://mariadb.com/kb/en/the-mariadb-library/resultset/
		 * https://github.com/MariaDB/mariadb-connector-j/blob/5fa814ac6e1b4c9cb6d141bd221cbd5fc45c8a78/src/main/java/org/mariadb/jdbc/internal/com/read/resultset/SelectResultSet.java#L443-L444
		 */
		public static bool IsOk(ReadOnlySpan<byte> span, bool deprecateEof) =>
			span.Length > 0 &&
				((span.Length > 6 && span[0] == Signature) ||
				(deprecateEof && span.Length < 0xFF_FFFF && span[0] == EofPayload.Signature));

		public static OkPayloadClassSpan Create(ReadOnlySpan<byte> span) => Create(span, false);

		public static OkPayloadClassSpan Create(ReadOnlySpan<byte> span, bool deprecateEof)
		{
			var reader = new ByteArrayReader(span);
			var signature = reader.ReadByte();
			if (signature != Signature && (!deprecateEof || signature != EofPayload.Signature))
				throw new FormatException("Expected to read 0x00 or 0xFE but got 0x{0:X2}".FormatInvariant(signature));
			var affectedRowCount = checked((int) reader.ReadLengthEncodedInteger());
			var lastInsertId = reader.ReadLengthEncodedInteger();
			var serverStatus = (ServerStatus) reader.ReadUInt16();
			var warningCount = (int) reader.ReadUInt16();
			string newSchema = null;

			if ((serverStatus & ServerStatus.SessionStateChanged) == ServerStatus.SessionStateChanged)
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

			return new OkPayloadClassSpan(affectedRowCount, lastInsertId, serverStatus, warningCount, newSchema);
		}

		private OkPayloadClassSpan(int affectedRowCount, ulong lastInsertId, ServerStatus serverStatus, int warningCount, string newSchema)
		{
			AffectedRowCount = affectedRowCount;
			LastInsertId = lastInsertId;
			ServerStatus = serverStatus;
			WarningCount = warningCount;
			NewSchema = newSchema;
		}

		static readonly OkPayloadClassSpan s_autoCommitOk = new OkPayloadClassSpan(0, 0, ServerStatus.AutoCommit, 0, null);
		static readonly OkPayloadClassSpan s_autoCommitSessionStateChangedOk = new OkPayloadClassSpan(0, 0, ServerStatus.AutoCommit | ServerStatus.SessionStateChanged, 0, null);
	}

	internal readonly struct OkPayloadStructSpan
	{
		public int AffectedRowCount { get; }
		public ulong LastInsertId { get; }
		public ServerStatus ServerStatus { get; }
		public int WarningCount { get; }
		public string NewSchema { get; }

		public const byte Signature = 0x00;

		/* See
		 * http://web.archive.org/web/20160604101747/http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
		 * https://mariadb.com/kb/en/the-mariadb-library/resultset/
		 * https://github.com/MariaDB/mariadb-connector-j/blob/5fa814ac6e1b4c9cb6d141bd221cbd5fc45c8a78/src/main/java/org/mariadb/jdbc/internal/com/read/resultset/SelectResultSet.java#L443-L444
		 */
		public static bool IsOk(ReadOnlySpan<byte> span, bool deprecateEof) =>
			span.Length > 0 &&
				((span.Length > 6 && span[0] == Signature) ||
				(deprecateEof && span.Length < 0xFF_FFFF && span[0] == EofPayload.Signature));

		public static OkPayloadStructSpan Create(ReadOnlySpan<byte> span) => Create(span, false);

		public static OkPayloadStructSpan Create(ReadOnlySpan<byte> span, bool deprecateEof)
		{
			var reader = new ByteArrayReader(span);
			var signature = reader.ReadByte();
			if (signature != Signature && (!deprecateEof || signature != EofPayload.Signature))
				throw new FormatException("Expected to read 0x00 or 0xFE but got 0x{0:X2}".FormatInvariant(signature));
			var affectedRowCount = checked((int) reader.ReadLengthEncodedInteger());
			var lastInsertId = reader.ReadLengthEncodedInteger();
			var serverStatus = (ServerStatus) reader.ReadUInt16();
			var warningCount = (int) reader.ReadUInt16();
			string newSchema = null;

			if ((serverStatus & ServerStatus.SessionStateChanged) == ServerStatus.SessionStateChanged)
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

			return new OkPayloadStructSpan(affectedRowCount, lastInsertId, serverStatus, warningCount, newSchema);
		}

		private OkPayloadStructSpan(int affectedRowCount, ulong lastInsertId, ServerStatus serverStatus, int warningCount, string newSchema)
		{
			AffectedRowCount = affectedRowCount;
			LastInsertId = lastInsertId;
			ServerStatus = serverStatus;
			WarningCount = warningCount;
			NewSchema = newSchema;
		}

		static readonly OkPayloadStructSpan s_autoCommitOk = new OkPayloadStructSpan(0, 0, ServerStatus.AutoCommit, 0, null);
		static readonly OkPayloadStructSpan s_autoCommitSessionStateChangedOk = new OkPayloadStructSpan(0, 0, ServerStatus.AutoCommit | ServerStatus.SessionStateChanged, 0, null);
	}

	internal ref struct ByteArrayReader
	{
		public ByteArrayReader(ReadOnlySpan<byte> buffer)
		{
			m_buffer = buffer;
			m_offset = 0;
			m_maxOffset = buffer.Length;
		}

		public ByteArrayReader(ArraySegment<byte> arraySegment)
			: this(arraySegment.AsSpan())
		{
		}

		public int Offset
		{
			get => m_offset;
			set => m_offset = value >= 0 && value <= m_maxOffset ? value : throw new ArgumentOutOfRangeException(nameof(value), "value must be between 0 and {0}".FormatInvariant(m_maxOffset));
		}

		public byte ReadByte()
		{
			VerifyRead(1);
			return m_buffer[m_offset++];
		}

		public void ReadByte(byte value)
		{
			if (ReadByte() != value)
				throw new FormatException("Expected to read 0x{0:X2} but got 0x{1:X2}".FormatInvariant(value, m_buffer[m_offset - 1]));
		}

		public short ReadInt16()
		{
			VerifyRead(2);
			var result = BinaryPrimitives.ReadInt16LittleEndian(m_buffer.Slice(m_offset));
			m_offset += 2;
			return result;
		}

		public ushort ReadUInt16()
		{
			VerifyRead(2);
			var result = BinaryPrimitives.ReadUInt16LittleEndian(m_buffer.Slice(m_offset));
			m_offset += 2;
			return result;
		}

		public int ReadInt32()
		{
			VerifyRead(4);
			var result = BinaryPrimitives.ReadInt32LittleEndian(m_buffer.Slice(m_offset));
			m_offset += 4;
			return result;
		}

		public uint ReadUInt32()
		{
			VerifyRead(4);
			var result = BinaryPrimitives.ReadUInt32LittleEndian(m_buffer.Slice(m_offset));
			m_offset += 4;
			return result;
		}

		public uint ReadFixedLengthUInt32(int length)
		{
			if (length <= 0 || length > 4)
				throw new ArgumentOutOfRangeException(nameof(length));
			VerifyRead(length);
			uint result = 0;
			for (int i = 0; i < length; i++)
				result |= ((uint) m_buffer[m_offset + i]) << (8 * i);
			m_offset += length;
			return result;
		}

		public ulong ReadFixedLengthUInt64(int length)
		{
			if (length <= 0 || length > 8)
				throw new ArgumentOutOfRangeException(nameof(length));
			VerifyRead(length);
			ulong result = 0;
			for (int i = 0; i < length; i++)
				result |= ((ulong) m_buffer[m_offset + i]) << (8 * i);
			m_offset += length;
			return result;
		}

		public ReadOnlySpan<byte> ReadNullTerminatedByteString()
		{
			int index = m_offset;
			while (index < m_maxOffset && m_buffer[index] != 0)
				index++;
			if (index == m_maxOffset)
				throw new FormatException("Read past end of buffer looking for NUL.");
			var substring = m_buffer.Slice(m_offset, index - m_offset);
			m_offset = index + 1;
			return substring;
		}

		public ReadOnlySpan<byte> ReadNullOrEofTerminatedByteString()
		{
			int index = m_offset;
			while (index < m_maxOffset && m_buffer[index] != 0)
				index++;
			var substring = m_buffer.Slice(m_offset, index - m_offset);
			if (index < m_maxOffset && m_buffer[index] == 0)
				index++;
			m_offset = index;
			return substring;
		}

		public ReadOnlySpan<byte> ReadByteString(int length)
		{
			VerifyRead(length);
			var result = m_buffer.Slice(m_offset, length);
			m_offset += length;
			return result;
		}

		public ulong ReadLengthEncodedInteger()
		{
			byte encodedLength = m_buffer[m_offset++];
			switch (encodedLength)
			{
			case 0xFB:
				throw new FormatException("Length-encoded integer cannot have 0xFB prefix byte.");
			case 0xFC:
				return ReadFixedLengthUInt32(2);
			case 0xFD:
				return ReadFixedLengthUInt32(3);
			case 0xFE:
				return ReadFixedLengthUInt64(8);
			case 0xFF:
				throw new FormatException("Length-encoded integer cannot have 0xFF prefix byte.");
			default:
				return encodedLength;
			}
		}

		public int ReadLengthEncodedIntegerOrNull()
		{
			if (m_buffer[m_offset] == 0xFB)
			{
				// "NULL is sent as 0xfb" (https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow)
				m_offset++;
				return -1;
			}
			return checked((int) ReadLengthEncodedInteger());
		}

		public ReadOnlySpan<byte> ReadLengthEncodedByteString()
		{
			var length = checked((int) ReadLengthEncodedInteger());
			var result = m_buffer.Slice(m_offset, length);
			m_offset += length;
			return result;
		}

		public int BytesRemaining => m_maxOffset - m_offset;

		private void VerifyRead(int length)
		{
			if (m_offset + length > m_maxOffset)
				throw new InvalidOperationException("Read past end of buffer.");
		}

		readonly ReadOnlySpan<byte> m_buffer;
		readonly int m_maxOffset;
		int m_offset;
	}

	internal enum SessionTrackKind : byte
	{
		/// <summary>
		/// SESSION_TRACK_SYSTEM_VARIABLES: one or more system variables changed
		/// </summary>
		SystemVariables = 0,

		/// <summary>
		/// SESSION_TRACK_SCHEMA: schema changed
		/// </summary>
		Schema = 1,

		/// <summary>
		/// SESSION_TRACK_STATE_CHANGE: "track state change" changed
		/// </summary>
		StateChange = 2,

		/// <summary>
		/// SESSION_TRACK_GTIDS: "track GTIDs" changed
		/// </summary>
		Gtids = 3,
	}

	[Flags]
	internal enum ServerStatus : ushort
	{
		/// <summary>
		/// A transaction is active.
		/// </summary>
		InTransaction = 1,

		/// <summary>
		/// Auto-commit is enabled
		/// </summary>
		AutoCommit = 2,

		MoreResultsExist = 8,

		NoGoodIndexUsed = 0x10,

		NoIndexUsed = 0x20,

		/// <summary>
		/// Used by Binary Protocol Resultset to signal that COM_STMT_FETCH must be used to fetch the row-data.
		/// </summary>
		CursorExists = 0x40,

		LastRowSent = 0x80,

		DatabaseDropped = 0x100,

		NoBackslashEscapes = 0x200,

		MetadataChanged = 0x400,

		QueryWasSlow = 0x800,

		PsOutParams = 0x1000,

		/// <summary>
		/// In a read-only transaction.
		/// </summary>
		InReadOnlyTransaction = 0x2000,

		/// <summary>
		/// Connection state information has changed.
		/// </summary>
		SessionStateChanged = 0x4000,
	}

	internal static class EofPayload
	{
		public const byte Signature = 0xFE;
	}

	internal static class Utility
	{
		public static string FormatInvariant(this string format, params object[] args) =>
			string.Format(CultureInfo.InvariantCulture, format, args);

		public static string GetString(this Encoding encoding, ReadOnlySpan<byte> span)
		{
			if (span.Length == 0)
				return "";
			unsafe
			{
				fixed (byte* ptr = span)
					return encoding.GetString(ptr, span.Length);
			}
		}
	}
}
