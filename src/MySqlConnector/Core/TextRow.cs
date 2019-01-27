using System;
using System.Buffers.Text;
using System.Text;
using MySql.Data.MySqlClient;
using MySql.Data.Types;
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Payloads;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Core
{
	internal sealed class TextRow : Row
	{
		public TextRow(ResultSet resultSet)
			: base(resultSet)
		{
		}

		protected override Row CloneCore() => new TextRow(ResultSet);

		protected override void GetDataOffsets(ReadOnlySpan<byte> data, int[] dataOffsets, int[] dataLengths)
		{
			var reader = new ByteArrayReader(data);
			for (var column = 0; column < dataOffsets.Length; column++)
			{
				var length = reader.ReadLengthEncodedIntegerOrNull();
				dataLengths[column] = length == -1 ? 0 : length;
				dataOffsets[column] = length == -1 ? -1 : reader.Offset;
				reader.Offset += dataLengths[column];
			}
		}

		protected override object GetValueCore(ReadOnlySpan<byte> data, ColumnDefinitionPayload columnDefinition)
		{
			var isUnsigned = (columnDefinition.ColumnFlags & ColumnFlags.Unsigned) != 0;
			switch (columnDefinition.ColumnType)
			{
			case ColumnType.Tiny:
				var value = ParseInt32(data);
				if (Connection.TreatTinyAsBoolean && columnDefinition.ColumnLength == 1 && !isUnsigned)
					return value != 0;
				return isUnsigned ? (object) (byte) value : (sbyte) value;

			case ColumnType.Int24:
			case ColumnType.Long:
				return isUnsigned ? (object) ParseUInt32(data) : ParseInt32(data);

			case ColumnType.Longlong:
				return isUnsigned ? (object) ParseUInt64(data) : ParseInt64(data);

			case ColumnType.Bit:
				// BIT column is transmitted as MSB byte array
				ulong bitValue = 0;
				for (int i = 0; i < data.Length; i++)
					bitValue = bitValue * 256 + data[i];
				return bitValue;

			case ColumnType.String:
				if (Connection.GuidFormat == MySqlGuidFormat.Char36 && columnDefinition.ColumnLength / ProtocolUtility.GetBytesPerCharacter(columnDefinition.CharacterSet) == 36)
					return Utf8Parser.TryParse(data, out Guid guid, out int guid36BytesConsumed, 'D') && guid36BytesConsumed == 36 ? guid : throw new FormatException();
				if (Connection.GuidFormat == MySqlGuidFormat.Char32 && columnDefinition.ColumnLength / ProtocolUtility.GetBytesPerCharacter(columnDefinition.CharacterSet) == 32)
					return Utf8Parser.TryParse(data, out Guid guid, out int guid32BytesConsumed, 'N') && guid32BytesConsumed == 32 ? guid : throw new FormatException();
				goto case ColumnType.VarString;

			case ColumnType.VarString:
			case ColumnType.VarChar:
			case ColumnType.TinyBlob:
			case ColumnType.Blob:
			case ColumnType.MediumBlob:
			case ColumnType.LongBlob:
				if (columnDefinition.CharacterSet == CharacterSet.Binary)
				{
					var guidFormat = Connection.GuidFormat;
					if ((guidFormat == MySqlGuidFormat.Binary16 || guidFormat == MySqlGuidFormat.TimeSwapBinary16 || guidFormat == MySqlGuidFormat.LittleEndianBinary16) && columnDefinition.ColumnLength == 16)
						return CreateGuidFromBytes(guidFormat, data);

					return data.ToArray();
				}
				return Encoding.UTF8.GetString(data);

			case ColumnType.Json:
				return Encoding.UTF8.GetString(data);

			case ColumnType.Short:
				return isUnsigned ? (object) ParseUInt16(data) : ParseInt16(data);

			case ColumnType.Date:
			case ColumnType.DateTime:
			case ColumnType.Timestamp:
				return ParseDateTime(data);

			case ColumnType.Time:
				return Utility.ParseTimeSpan(data);

			case ColumnType.Year:
				return ParseInt32(data);

			case ColumnType.Float:
				return !Utf8Parser.TryParse(data, out float floatValue, out var floatBytesConsumed) || floatBytesConsumed != data.Length ? throw new FormatException() : floatValue;

			case ColumnType.Double:
				return !Utf8Parser.TryParse(data, out double doubleValue, out var doubleBytesConsumed) || doubleBytesConsumed != data.Length ? throw new FormatException() : doubleValue;

			case ColumnType.Decimal:
			case ColumnType.NewDecimal:
				return Utf8Parser.TryParse(data, out decimal decimalValue, out int bytesConsumed) && bytesConsumed == data.Length ? decimalValue : throw new FormatException();

			case ColumnType.Geometry:
				return data.ToArray();

			default:
				throw new NotImplementedException("Reading {0} not implemented".FormatInvariant(columnDefinition.ColumnType));
			}
		}

		private static short ParseInt16(ReadOnlySpan<byte> data) =>
			!Utf8Parser.TryParse(data, out short value, out var bytesConsumed) || bytesConsumed != data.Length ? throw new FormatException() : value;

		private static ushort ParseUInt16(ReadOnlySpan<byte> data) =>
			!Utf8Parser.TryParse(data, out ushort value, out var bytesConsumed) || bytesConsumed != data.Length ? throw new FormatException() : value;

		private static int ParseInt32(ReadOnlySpan<byte> data) =>
			!Utf8Parser.TryParse(data, out int value, out var bytesConsumed) || bytesConsumed != data.Length ? throw new FormatException() : value;

		private static uint ParseUInt32(ReadOnlySpan<byte> data) =>
			!Utf8Parser.TryParse(data, out uint value, out var bytesConsumed) || bytesConsumed != data.Length ? throw new FormatException() : value;

		private static long ParseInt64(ReadOnlySpan<byte> data) =>
			!Utf8Parser.TryParse(data, out long value, out var bytesConsumed) || bytesConsumed != data.Length ? throw new FormatException() : value;

		private static ulong ParseUInt64(ReadOnlySpan<byte> data) =>
			!Utf8Parser.TryParse(data, out ulong value, out var bytesConsumed) || bytesConsumed != data.Length ? throw new FormatException() : value;

		private object ParseDateTime(ReadOnlySpan<byte> value)
		{
			Exception? exception = null;
			if (!Utf8Parser.TryParse(value, out int year, out var bytesConsumed) || bytesConsumed != 4)
				goto InvalidDateTime;
			if (value.Length < 5 || value[4] != 45)
				goto InvalidDateTime;
			if (!Utf8Parser.TryParse(value.Slice(5), out int month, out bytesConsumed) || bytesConsumed != 2)
				goto InvalidDateTime;
			if (value.Length < 8 || value[7] != 45)
				goto InvalidDateTime;
			if (!Utf8Parser.TryParse(value.Slice(8), out int day, out bytesConsumed) || bytesConsumed != 2)
				goto InvalidDateTime;

			if (year == 0 && month == 0 && day == 0)
			{
				if (Connection.ConvertZeroDateTime)
					return DateTime.MinValue;
				if (Connection.AllowZeroDateTime)
					return new MySqlDateTime();
				throw new InvalidCastException("Unable to convert MySQL date/time to System.DateTime.");
			}

			int hour, minute, second, microseconds;
			if (value.Length == 10)
			{
				hour = 0;
				minute = 0;
				second = 0;
				microseconds = 0;
			}
			else
			{
				if (value[10] != 32)
					goto InvalidDateTime;
				if (!Utf8Parser.TryParse(value.Slice(11), out hour, out bytesConsumed) || bytesConsumed != 2)
					goto InvalidDateTime;
				if (value.Length < 14 || value[13] != 58)
					goto InvalidDateTime;
				if (!Utf8Parser.TryParse(value.Slice(14), out minute, out bytesConsumed) || bytesConsumed != 2)
					goto InvalidDateTime;
				if (value.Length < 17 || value[16] != 58)
					goto InvalidDateTime;
				if (!Utf8Parser.TryParse(value.Slice(17), out second, out bytesConsumed) || bytesConsumed != 2)
					goto InvalidDateTime;

				if (value.Length == 19)
				{
					microseconds = 0;
				}
				else
				{
					if (value[19] != 46)
						goto InvalidDateTime;

					if (!Utf8Parser.TryParse(value.Slice(20), out microseconds, out bytesConsumed) || bytesConsumed != value.Length - 20)
						goto InvalidDateTime;
					for (; bytesConsumed < 6; bytesConsumed++)
						microseconds *= 10;
				}
			}

			try
			{
				return Connection.AllowZeroDateTime ? (object) new MySqlDateTime(year, month, day, hour, minute, second, microseconds) :
					new DateTime(year, month, day, hour, minute, second, microseconds / 1000, Connection.DateTimeKind).AddTicks(microseconds % 1000 * 10);
			}
			catch (Exception ex)
			{
				exception = ex;
			}

InvalidDateTime:
			throw new FormatException("Couldn't interpret '{0}' as a valid DateTime".FormatInvariant(Encoding.UTF8.GetString(value)), exception);
		}
	}
}
