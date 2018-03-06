#define UTF8PARSER
using System;
using System.Buffers.Text;
using System.Globalization;
using System.Text;
using MySql.Data.MySqlClient;
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Core
{
	internal sealed class Row : IDisposable
	{
		public Row(ResultSet resultSet) => ResultSet = resultSet;

		public void SetData(int[] dataLengths, int[] dataOffsets, ArraySegment<byte> payload)
		{
			m_dataLengths = dataLengths;
			m_dataOffsets = dataOffsets;
			m_payload = payload;
		}

		public void BufferData()
		{
			// by default, m_payload, m_dataLengths, and m_dataOffsets are re-used to save allocations
			// if the row is going to be buffered, we need to copy them
			// since m_payload can span multiple rows, offests must recalculated to include only this row

			var bufferDataLengths = new int[m_dataLengths.Length];
			Buffer.BlockCopy(m_dataLengths, 0, bufferDataLengths, 0, m_dataLengths.Length * sizeof(int));
			m_dataLengths = bufferDataLengths;

			var bufferDataOffsets = new int[m_dataOffsets.Length];
			for (var i = 0; i < m_dataOffsets.Length; i++)
			{
				// a -1 offset denotes null, only adjust positive offsets
				if (m_dataOffsets[i] >= 0)
					bufferDataOffsets[i] = m_dataOffsets[i] - m_payload.Offset;
				else
					bufferDataOffsets[i] = m_dataOffsets[i];
			}
			m_dataOffsets = bufferDataOffsets;

			var bufferedPayload = new byte[m_payload.Count];
			Buffer.BlockCopy(m_payload.Array, m_payload.Offset, bufferedPayload, 0, m_payload.Count);
			m_payload = new ArraySegment<byte>(bufferedPayload);
		}

		public void Dispose() => ClearData();

		public void ClearData()
		{
			m_dataLengths = null;
			m_dataOffsets = null;
			m_payload = default(ArraySegment<byte>);
		}

		public bool GetBoolean(int ordinal)
		{
			var value = GetValue(ordinal);
			if (value is bool)
				return (bool) value;

			if (value is sbyte)
				return (sbyte) value != 0;
			if (value is byte)
				return (byte) value != 0;
			if (value is short)
				return (short) value != 0;
			if (value is ushort)
				return (ushort) value != 0;
			if (value is int)
				return (int) value != 0;
			if (value is uint)
				return (uint) value != 0;
			if (value is long)
				return (long) value != 0;
			if (value is ulong)
				return (ulong) value != 0;
			return (bool) value;
		}

		public sbyte GetSByte(int ordinal) => (sbyte) GetValue(ordinal);

		public byte GetByte(int ordinal) => (byte) GetValue(ordinal);

		public long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
		{
			if (m_dataOffsets[ordinal] == -1)
				throw new InvalidCastException("Column is NULL.");

			var column = ResultSet.ColumnDefinitions[ordinal];
			var columnType = column.ColumnType;
			if ((column.ColumnFlags & ColumnFlags.Binary) == 0 ||
				(columnType != ColumnType.String && columnType != ColumnType.VarString && columnType != ColumnType.TinyBlob &&
				columnType != ColumnType.Blob && columnType != ColumnType.MediumBlob && columnType != ColumnType.LongBlob))
			{
				throw new InvalidCastException("Can't convert {0} to bytes.".FormatInvariant(columnType));
			}

			if (buffer == null)
			{
				// this isn't required by the DbDataReader.GetBytes API documentation, but is what mysql-connector-net does
				// (as does SqlDataReader: http://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqldatareader.getbytes.aspx)
				return m_dataLengths[ordinal];
			}

			CheckBufferArguments(dataOffset, buffer, bufferOffset, length);

			var offset = (int) dataOffset;
			var lengthToCopy = Math.Max(0, Math.Min(m_dataLengths[ordinal] - offset, length));
			Buffer.BlockCopy(m_payload.Array, m_dataOffsets[ordinal] + offset, buffer, bufferOffset, lengthToCopy);
			return lengthToCopy;
		}

		public char GetChar(int ordinal) => (char) GetValue(ordinal);

		public long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
		{
			var value = GetString(ordinal);
			if (buffer == null)
				return value.Length;

			CheckBufferArguments(dataOffset, buffer, bufferOffset, length);

			var offset = (int) dataOffset;
			var lengthToCopy = Math.Max(0, Math.Min(value.Length - offset, length));
			if (lengthToCopy > 0)
				value.CopyTo(offset, buffer, bufferOffset, lengthToCopy);
			return lengthToCopy;
		}

		public Guid GetGuid(int ordinal)
		{
			var value = GetValue(ordinal);
			if (value is Guid guid)
				return guid;

			if (value is string stringValue && Guid.TryParse(stringValue, out guid))
				return guid;

			if (value is byte[] bytes && bytes.Length == 16)
				return new Guid(bytes);

			throw new InvalidCastException("The value could not be converted to a GUID: {0}".FormatInvariant(value));
		}

		public short GetInt16(int ordinal)
		{
			var value = GetValue(ordinal);
			if (value is short)
				return (short) value;

			if (value is sbyte)
				return (sbyte) value;
			if (value is byte)
				return (byte) value;
			if (value is ushort)
				return checked((short) (ushort) value);
			if (value is int)
				return checked((short) (int) value);
			if (value is uint)
				return checked((short) (uint) value);
			if (value is long)
				return checked((short) (long) value);
			if (value is ulong)
				return checked((short) (ulong) value);
			if (value is decimal)
				return (short) (decimal) value;
			return (short) value;
		}

		public int GetInt32(int ordinal)
		{
#if UTF8PARSER
			if (m_dataOffsets[ordinal] == -1)
				throw new InvalidCastException();
			var columnDefinition = ResultSet.ColumnDefinitions[ordinal];
			var columnType = columnDefinition.ColumnType;
			if (columnType == ColumnType.Long || columnType == ColumnType.Longlong || columnType == ColumnType.Int24 ||
			    columnType == ColumnType.Short || (columnType == ColumnType.Tiny && !(Connection.TreatTinyAsBoolean && columnDefinition.ColumnLength == 1)) ||
			    columnType == ColumnType.Decimal || columnType == ColumnType.NewDecimal || columnType == ColumnType.Year)
			{
				var span = new ReadOnlySpan<byte>(m_payload.Array, m_dataOffsets[ordinal], m_dataLengths[ordinal]);
				if (Utf8Parser.TryParse(span, out int value, out var bytesConsumed) && bytesConsumed == m_dataLengths[ordinal])
					return value;
				throw new FormatException();
			}

			throw new InvalidCastException();
#else
			var value = GetValue(ordinal);
			if (value is int)
				return (int) value;

			if (value is sbyte)
				return (sbyte) value;
			if (value is byte)
				return (byte) value;
			if (value is short)
				return (short) value;
			if (value is ushort)
				return (ushort) value;
			if (value is uint)
				return checked((int) (uint) value);
			if (value is long)
				return checked((int) (long) value);
			if (value is ulong)
				return checked((int) (ulong) value);
			if (value is decimal)
				return (int) (decimal) value;
			return (int) value;
#endif
		}

		public long GetInt64(int ordinal)
		{
			var value = GetValue(ordinal);
			if (value is long)
				return (long) value;

			if (value is sbyte)
				return (sbyte) value;
			if (value is byte)
				return (byte) value;
			if (value is short)
				return (short) value;
			if (value is ushort)
				return (ushort) value;
			if (value is int)
				return (int) value;
			if (value is uint)
				return (uint) value;
			if (value is ulong)
				return checked((long) (ulong) value);
			if (value is decimal)
				return (long) (decimal) value;
			return (long) value;
		}

		public ushort GetUInt16(int ordinal)
		{
			var value = GetValue(ordinal);
			if (value is ushort)
				return (ushort) value;

			if (value is sbyte)
				return checked((ushort) (sbyte) value);
			if (value is byte)
				return (byte) value;
			if (value is short)
				return checked((ushort) (short) value);
			if (value is int)
				return checked((ushort) (int) value);
			if (value is uint)
				return checked((ushort) (uint) value);
			if (value is long)
				return checked((ushort) (long) value);
			if (value is ulong)
				return checked((ushort) (ulong) value);
			if (value is decimal)
				return (ushort) (decimal) value;
			return (ushort) value;
		}

		public uint GetUInt32(int ordinal)
		{
			var value = GetValue(ordinal);
			if (value is uint)
				return (uint) value;

			if (value is sbyte)
				return checked((uint) (sbyte) value);
			if (value is byte)
				return (byte) value;
			if (value is short)
				return checked((uint) (short) value);
			if (value is ushort)
				return (ushort) value;
			if (value is int)
				return checked((uint) (int) value);
			if (value is long)
				return checked((uint) (long) value);
			if (value is ulong)
				return checked((uint) (ulong) value);
			if (value is decimal)
				return (uint) (decimal) value;
			return (uint) value;
		}

		public ulong GetUInt64(int ordinal)
		{
			var value = GetValue(ordinal);
			if (value is ulong)
				return (ulong) value;

			if (value is sbyte)
				return checked((ulong) (sbyte) value);
			if (value is byte)
				return (byte) value;
			if (value is short)
				return checked((ulong) (short) value);
			if (value is ushort)
				return (ushort) value;
			if (value is int)
				return checked((ulong) (int) value);
			if (value is uint)
				return (uint) value;
			if (value is long)
				return checked((ulong) (long) value);
			if (value is decimal)
				return (ulong) (decimal) value;
			return (ulong) value;
		}

		public DateTime GetDateTime(int ordinal) => (DateTime) GetValue(ordinal);

		public DateTimeOffset GetDateTimeOffset(int ordinal) => new DateTimeOffset(DateTime.SpecifyKind(GetDateTime(ordinal), DateTimeKind.Utc));

		public string GetString(int ordinal) => (string) GetValue(ordinal);

		public decimal GetDecimal(int ordinal) => (decimal) GetValue(ordinal);

		public double GetDouble(int ordinal)
		{
			var value = GetValue(ordinal);
			return value is float floatValue ? floatValue : (double) value;
		}

		public float GetFloat(int ordinal) => (float) GetValue(ordinal);

		public int GetValues(object[] values)
		{
			int count = Math.Min(values.Length, ResultSet.ColumnDefinitions.Length);
			for (int i = 0; i < count; i++)
				values[i] = GetValue(i);
			return count;
		}

		public bool IsDBNull(int ordinal) => m_dataOffsets[ordinal] == -1;

		public object this[int ordinal] => GetValue(ordinal);

		public object this[string name] => GetValue(ResultSet.GetOrdinal(name));

		public object GetValue(int ordinal)
		{
			if (ordinal < 0 || ordinal > ResultSet.ColumnDefinitions.Length)
				throw new ArgumentOutOfRangeException(nameof(ordinal), "value must be between 0 and {0}.".FormatInvariant(ResultSet.ColumnDefinitions.Length));

			if (m_dataOffsets[ordinal] == -1)
				return DBNull.Value;

			var data = new ArraySegment<byte>(m_payload.Array, m_dataOffsets[ordinal], m_dataLengths[ordinal]);
			var columnDefinition = ResultSet.ColumnDefinitions[ordinal];
			var isUnsigned = (columnDefinition.ColumnFlags & ColumnFlags.Unsigned) != 0;
			switch (columnDefinition.ColumnType)
			{
				case ColumnType.Tiny:
					var value = int.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);
					if (Connection.TreatTinyAsBoolean && columnDefinition.ColumnLength == 1)
						return value != 0;
					return isUnsigned ? (object) (byte) value : (sbyte) value;

				case ColumnType.Int24:
				case ColumnType.Long:
					return isUnsigned ? (object) uint.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture) :
						int.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

				case ColumnType.Longlong:
					return isUnsigned ? (object) ulong.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture) :
						long.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

				case ColumnType.Bit:
					// BIT column is transmitted as MSB byte array
					ulong bitValue = 0;
					for (int i = 0; i < m_dataLengths[ordinal]; i++)
						bitValue = bitValue * 256 + m_payload.Array[m_dataOffsets[ordinal] + i];
					return bitValue;

				case ColumnType.String:
					if (!Connection.OldGuids && columnDefinition.ColumnLength / ProtocolUtility.GetBytesPerCharacter(columnDefinition.CharacterSet) == 36)
						return Guid.Parse(Encoding.UTF8.GetString(data));
					goto case ColumnType.VarString;

				case ColumnType.VarString:
				case ColumnType.VarChar:
				case ColumnType.TinyBlob:
				case ColumnType.Blob:
				case ColumnType.MediumBlob:
				case ColumnType.LongBlob:
					if (columnDefinition.CharacterSet == CharacterSet.Binary)
					{
						var result = new byte[m_dataLengths[ordinal]];
						Buffer.BlockCopy(m_payload.Array, m_dataOffsets[ordinal], result, 0, result.Length);
						return Connection.OldGuids && columnDefinition.ColumnLength == 16 ? (object) new Guid(result) : result;
					}
					return Encoding.UTF8.GetString(data);

				case ColumnType.Json:
					return Encoding.UTF8.GetString(data);

				case ColumnType.Short:
					return isUnsigned ? (object) ushort.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture) :
						short.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

				case ColumnType.Date:
				case ColumnType.DateTime:
				case ColumnType.Timestamp:
					return ParseDateTime(data);

				case ColumnType.Time:
					return ParseTimeSpan(data);

				case ColumnType.Year:
					return int.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

				case ColumnType.Float:
					return float.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

				case ColumnType.Double:
					return double.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

				case ColumnType.Decimal:
				case ColumnType.NewDecimal:
					return decimal.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

				default:
					throw new NotImplementedException("Reading {0} not implemented".FormatInvariant(columnDefinition.ColumnType));
			}
		}

		private static void CheckBufferArguments<T>(long dataOffset, T[] buffer, int bufferOffset, int length)
		{
			if (dataOffset < 0)
				throw new ArgumentOutOfRangeException(nameof(dataOffset), dataOffset, "dataOffset must be non-negative");
			if (dataOffset > int.MaxValue)
				throw new ArgumentOutOfRangeException(nameof(dataOffset), dataOffset, "dataOffset must be a 32-bit integer");
			if (length < 0)
				throw new ArgumentOutOfRangeException(nameof(length), length, "length must be non-negative");
			if (bufferOffset < 0)
				throw new ArgumentOutOfRangeException(nameof(bufferOffset), bufferOffset, "bufferOffset must be non-negative");
			if (bufferOffset > buffer.Length)
				throw new ArgumentOutOfRangeException(nameof(bufferOffset), bufferOffset, "bufferOffset must be within the buffer");
			if (checked(bufferOffset + length) > buffer.Length)
				throw new ArgumentException("bufferOffset + length cannot exceed buffer.Length", nameof(length));
		}

		private DateTime ParseDateTime(ArraySegment<byte> value)
		{
			var parts = Encoding.UTF8.GetString(value).Split('-', ' ', ':', '.');

			var year = int.Parse(parts[0], CultureInfo.InvariantCulture);
			var month = int.Parse(parts[1], CultureInfo.InvariantCulture);
			var day = int.Parse(parts[2], CultureInfo.InvariantCulture);

			if (year == 0 && month == 0 && day == 0)
			{
				if (Connection.ConvertZeroDateTime)
					return DateTime.MinValue;
				throw new InvalidCastException("Unable to convert MySQL date/time to System.DateTime.");
			}

			if (parts.Length == 3)
				return new DateTime(year, month, day);

			var hour = int.Parse(parts[3], CultureInfo.InvariantCulture);
			var minute = int.Parse(parts[4], CultureInfo.InvariantCulture);
			var second = int.Parse(parts[5], CultureInfo.InvariantCulture);
			if (parts.Length == 6)
				return new DateTime(year, month, day, hour, minute, second);

			var microseconds = int.Parse(parts[6] + new string('0', 6 - parts[6].Length), CultureInfo.InvariantCulture);
			return new DateTime(year, month, day, hour, minute, second, microseconds / 1000).AddTicks(microseconds % 1000 * 10);
		}

		private static TimeSpan ParseTimeSpan(ArraySegment<byte> value)
		{
			var parts = Encoding.UTF8.GetString(value).Split(':', '.');

			var hours = int.Parse(parts[0], CultureInfo.InvariantCulture);
			var minutes = int.Parse(parts[1], CultureInfo.InvariantCulture);
			if (hours < 0)
				minutes = -minutes;
			var seconds = int.Parse(parts[2], CultureInfo.InvariantCulture);
			if (hours < 0)
				seconds = -seconds;
			if (parts.Length == 3)
				return new TimeSpan(hours, minutes, seconds);

			var microseconds = int.Parse(parts[3] + new string('0', 6 - parts[3].Length), CultureInfo.InvariantCulture);
			if (hours < 0)
				microseconds = -microseconds;
			return new TimeSpan(0, hours, minutes, seconds, microseconds / 1000) + TimeSpan.FromTicks(microseconds % 1000 * 10);
		}

		public readonly ResultSet ResultSet;
		public MySqlConnection Connection => ResultSet.Connection;

		ArraySegment<byte> m_payload;
		int[] m_dataLengths;
		int[] m_dataOffsets;
	}
}
