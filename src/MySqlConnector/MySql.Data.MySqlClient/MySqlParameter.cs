using System;
using System.Buffers.Text;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using MySql.Data.Types;
using MySqlConnector.Core;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySql.Data.MySqlClient
{
	public sealed class MySqlParameter : DbParameter, IDbDataParameter
#if !NETSTANDARD1_3
		, ICloneable
#endif
	{
		public MySqlParameter()
		{
			ParameterName = "";
			SourceColumn = "";
#if !NETSTANDARD1_3
			SourceVersion = DataRowVersion.Current;
#endif
			ResetDbType();
		}

		public MySqlParameter(string name, object? objValue)
			: this()
		{
			ParameterName = name;
			Value = objValue;
		}

		public MySqlParameter(string name, MySqlDbType mySqlDbType)
			: this(name, mySqlDbType, 0)
		{
		}

		public MySqlParameter(string name, MySqlDbType mySqlDbType, int size)
			: this(name, mySqlDbType, size, "")
		{
		}

		public MySqlParameter(string name, MySqlDbType mySqlDbType, int size, string sourceColumn)
		{
			ParameterName = name;
			MySqlDbType = mySqlDbType;
			Size = size;
			SourceColumn = sourceColumn;
#if !NETSTANDARD1_3
			SourceVersion = DataRowVersion.Current;
#endif
		}

#if !NETSTANDARD1_3
		public MySqlParameter(string name, MySqlDbType mySqlDbType, int size, ParameterDirection direction, bool isNullable, byte precision, byte scale, string sourceColumn, DataRowVersion sourceVersion, object? value)
			: this(name, mySqlDbType, size, sourceColumn)
		{
			Direction = direction;
			IsNullable = isNullable;
			Precision = precision;
			Scale = scale;
			SourceVersion = sourceVersion;
			Value = value;
		}
#endif

		public override DbType DbType
		{
			get => m_dbType;
			set
			{
				m_dbType = value;
				m_mySqlDbType = TypeMapper.Instance.GetMySqlDbTypeForDbType(value);
				HasSetDbType = true;
			}
		}

		public MySqlDbType MySqlDbType
		{
			get => m_mySqlDbType;
			set
			{
				m_dbType = TypeMapper.Instance.GetDbTypeForMySqlDbType(value);
				m_mySqlDbType = value;
				HasSetDbType = true;
			}
		}

		public override ParameterDirection Direction
		{
			get => m_direction.GetValueOrDefault(ParameterDirection.Input);
			set
			{
				if (value != ParameterDirection.Input && value != ParameterDirection.Output &&
					value != ParameterDirection.InputOutput && value != ParameterDirection.ReturnValue)
				{
					throw new ArgumentOutOfRangeException(nameof(value), "{0} is not a supported value for ParameterDirection".FormatInvariant(value));
				}
				m_direction = value;
			}
		}

		public override bool IsNullable { get; set; }

#if NET45
		public byte Precision { get; set; }
		public byte Scale { get; set; }
#else
		public override byte Precision { get; set; }
		public override byte Scale { get; set; }
#endif

		public override string? ParameterName
		{
			get => m_name;
			set
			{
				m_name = value;
				NormalizedParameterName = value is null ? null : NormalizeParameterName(value);
			}
		}

		public override int Size { get; set; }

		public override string SourceColumn { get; set; }

		public override bool SourceColumnNullMapping { get; set; }

#if !NETSTANDARD1_3
		public override DataRowVersion SourceVersion { get; set; }
#endif

		public override object? Value
		{
			get => m_value;
			set
			{
				m_value = value;
				if (!HasSetDbType && value != null)
				{
					var typeMapping = TypeMapper.Instance.GetDbTypeMapping(value.GetType());
					if (typeMapping != null)
					{
						m_dbType = typeMapping.DbTypes[0];
						m_mySqlDbType = TypeMapper.Instance.GetMySqlDbTypeForDbType(m_dbType);
					}
				}
			}
		}

		public override void ResetDbType()
		{
			m_mySqlDbType = MySqlDbType.VarChar;
			m_dbType = DbType.String;
			HasSetDbType = false;
		}

		public MySqlParameter Clone() => new MySqlParameter(this);

#if !NETSTANDARD1_3
		object ICloneable.Clone() => Clone();
#endif

		internal MySqlParameter WithParameterName(string parameterName) => new MySqlParameter(this, parameterName);

		private MySqlParameter(MySqlParameter other)
		{
			m_dbType = other.m_dbType;
			m_mySqlDbType = other.m_mySqlDbType;
			m_direction = other.m_direction;
			HasSetDbType = other.HasSetDbType;
			IsNullable = other.IsNullable;
			Size = other.Size;
			m_name = other.m_name;
			NormalizedParameterName = other.NormalizedParameterName;
			m_value = other.m_value;
			Precision = other.Precision;
			Scale = other.Scale;
			SourceColumn = other.SourceColumn;
			SourceColumnNullMapping = other.SourceColumnNullMapping;
#if !NETSTANDARD1_3
			SourceVersion = other.SourceVersion;
#endif
		}

		private MySqlParameter(MySqlParameter other, string parameterName)
			: this(other)
		{
			ParameterName = parameterName ?? throw new ArgumentNullException(nameof(parameterName));
		}

		internal bool HasSetDirection => m_direction.HasValue;

		internal bool HasSetDbType { get; set; }

		internal string? NormalizedParameterName { get; private set; }

		internal void AppendSqlString(ByteBufferWriter writer, StatementPreparerOptions options)
		{
			if (Value == null || Value == DBNull.Value)
			{
				writer.Write(s_nullBytes);
			}
			else if (Value is string stringValue)
			{
				writer.Write((byte) '\'');
				writer.Write(stringValue.Replace("\\", "\\\\").Replace("'", "\\'"));
				writer.Write((byte) '\'');
			}
			else if (Value is char charValue)
			{
				writer.Write((byte) '\'');
				switch (charValue)
				{
				case '\'':
				case '\\':
					writer.Write((byte) '\\');
					writer.Write((byte) charValue);
					break;

				default:
					writer.Write(charValue.ToString());
					break;
				}
				writer.Write((byte) '\'');
			}
			else if (Value is byte || Value is sbyte || Value is decimal)
			{
				writer.Write("{0}".FormatInvariant(Value));
			}
			else if (Value is short shortValue)
			{
				writer.WriteString(shortValue);
			}
			else if (Value is ushort ushortValue)
			{
				writer.WriteString(ushortValue);
			}
			else if (Value is int intValue)
			{
				writer.WriteString(intValue);
			}
			else if (Value is uint uintValue)
			{
				writer.WriteString(uintValue);
			}
			else if (Value is long longValue)
			{
				writer.WriteString(longValue);
			}
			else if (Value is ulong ulongValue)
			{
				writer.WriteString(ulongValue);
			}
			else if (Value is byte[] byteArrayValue)
			{
				// determine the number of bytes to be written
				var length = byteArrayValue.Length + s_binaryBytes.Length + 1;
				foreach (var by in byteArrayValue)
				{
					if (by == 0x27 || by == 0x5C)
						length++;
				}

				var span = writer.GetSpan(length);
				s_binaryBytes.CopyTo(span);
				var index = s_binaryBytes.Length;
				foreach (var by in byteArrayValue)
				{
					if (by == 0x27 || by == 0x5C)
						span[index++] = 0x5C;
					span[index++] = by;
				}
				span[index++] = 0x27;
				Debug.Assert(index == length, "index == length");
				writer.Advance(index);
			}
			else if (Value is bool boolValue)
			{
				writer.Write(boolValue ? s_trueBytes : s_falseBytes);
			}
			else if (Value is float || Value is double)
			{
				// NOTE: Utf8Formatter doesn't support "R"
				writer.Write("{0:R}".FormatInvariant(Value));
			}
			else if (Value is MySqlDateTime mySqlDateTimeValue)
			{
				if (mySqlDateTimeValue.IsValidDateTime)
					writer.Write("timestamp('{0:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'ffffff}')".FormatInvariant(mySqlDateTimeValue.GetDateTime()));
				else
					writer.Write("timestamp('0000-00-00')");
			}
			else if (Value is DateTime dateTimeValue)
			{
				if ((options & StatementPreparerOptions.DateTimeUtc) != 0 && dateTimeValue.Kind == DateTimeKind.Local)
					throw new MySqlException("DateTime.Kind must not be Local when DateTimeKind setting is Utc (parameter name: {0})".FormatInvariant(ParameterName));
				else if ((options & StatementPreparerOptions.DateTimeLocal) != 0 && dateTimeValue.Kind == DateTimeKind.Utc)
					throw new MySqlException("DateTime.Kind must not be Utc when DateTimeKind setting is Local (parameter name: {0})".FormatInvariant(ParameterName));

				writer.Write("timestamp('{0:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'ffffff}')".FormatInvariant(dateTimeValue));
			}
			else if (Value is DateTimeOffset dateTimeOffsetValue)
			{
				// store as UTC as it will be read as such when deserialized from a timespan column
				writer.Write("timestamp('{0:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'ffffff}')".FormatInvariant(dateTimeOffsetValue.UtcDateTime));
			}
			else if (Value is TimeSpan ts)
			{
				writer.Write("time '");
				if (ts.Ticks < 0)
				{
					writer.Write((byte) '-');
					ts = TimeSpan.FromTicks(-ts.Ticks);
				}
				writer.Write("{0}:{1:mm':'ss'.'ffffff}'".FormatInvariant(ts.Days * 24 + ts.Hours, ts));
			}
			else if (Value is Guid guidValue)
			{
				StatementPreparerOptions guidOptions = options & StatementPreparerOptions.GuidFormatMask;
				if (guidOptions == StatementPreparerOptions.GuidFormatBinary16 ||
					guidOptions == StatementPreparerOptions.GuidFormatTimeSwapBinary16 ||
					guidOptions == StatementPreparerOptions.GuidFormatLittleEndianBinary16)
				{
					var bytes = guidValue.ToByteArray();
					if (guidOptions != StatementPreparerOptions.GuidFormatLittleEndianBinary16)
					{
						Utility.SwapBytes(bytes, 0, 3);
						Utility.SwapBytes(bytes, 1, 2);
						Utility.SwapBytes(bytes, 4, 5);
						Utility.SwapBytes(bytes, 6, 7);

						if (guidOptions == StatementPreparerOptions.GuidFormatTimeSwapBinary16)
						{
							Utility.SwapBytes(bytes, 0, 4);
							Utility.SwapBytes(bytes, 1, 5);
							Utility.SwapBytes(bytes, 2, 6);
							Utility.SwapBytes(bytes, 3, 7);
							Utility.SwapBytes(bytes, 0, 2);
							Utility.SwapBytes(bytes, 1, 3);
						}
					}
					writer.Write(s_binaryBytes);
					foreach (var by in bytes)
					{
						if (by == 0x27 || by == 0x5C)
							writer.Write((byte) 0x5C);
						writer.Write(by);
					}
					writer.Write((byte) '\'');
				}
				else
				{
					var is32Characters = guidOptions == StatementPreparerOptions.GuidFormatChar32;
					var guidLength = is32Characters ? 34 : 38;
					var span = writer.GetSpan(guidLength);
					span[0] = 0x27;
					Utf8Formatter.TryFormat(guidValue, span.Slice(1), out _, is32Characters ? 'N' : 'D');
					span[guidLength - 1] = 0x27;
					writer.Advance(guidLength);
				}
			}
			else if (MySqlDbType == MySqlDbType.Int16)
			{
				writer.WriteString((short) Value);
			}
			else if (MySqlDbType == MySqlDbType.UInt16)
			{
				writer.WriteString((ushort) Value);
			}
			else if (MySqlDbType == MySqlDbType.Int32)
			{
				writer.WriteString((int) Value);
			}
			else if (MySqlDbType == MySqlDbType.UInt32)
			{
				writer.WriteString((uint) Value);
			}
			else if (MySqlDbType == MySqlDbType.Int64)
			{
				writer.WriteString((long) Value);
			}
			else if (MySqlDbType == MySqlDbType.UInt64)
			{
				writer.WriteString((ulong) Value);
			}
			else if ((MySqlDbType == MySqlDbType.String || MySqlDbType == MySqlDbType.VarChar) && HasSetDbType && Value is Enum)
			{
				writer.Write("'{0:G}'".FormatInvariant(Value));
			}
			else if (Value is Enum)
			{
				writer.Write("{0:d}".FormatInvariant(Value));
			}
			else
			{
				throw new NotSupportedException("Parameter type {0} (DbType: {1}) not currently supported. Value: {2}".FormatInvariant(Value.GetType().Name, DbType, Value));
			}
		}

		internal void AppendBinary(ByteBufferWriter writer, StatementPreparerOptions options)
		{
			if (Value == null || Value == DBNull.Value)
			{
				// stored in "null bitmap" only
			}
			else if (Value is string stringValue)
			{
				writer.WriteLengthEncodedString(stringValue);
			}
			else if (Value is char charValue)
			{
				writer.WriteLengthEncodedString(charValue.ToString());
			}
			else if (Value is sbyte sbyteValue)
			{
				writer.Write(unchecked((byte) sbyteValue));
			}
			else if (Value is byte byteValue)
			{
				writer.Write(byteValue);
			}
			else if (Value is bool boolValue)
			{
				writer.Write((byte) (boolValue ? 1 : 0));
			}
			else if (Value is short shortValue)
			{
				writer.Write(unchecked((ushort) shortValue));
			}
			else if (Value is ushort ushortValue)
			{
				writer.Write(ushortValue);
			}
			else if (Value is int intValue)
			{
				writer.Write(intValue);
			}
			else if (Value is uint uintValue)
			{
				writer.Write(uintValue);
			}
			else if (Value is long longValue)
			{
				writer.Write(unchecked((ulong) longValue));
			}
			else if (Value is ulong ulongValue)
			{
				writer.Write(ulongValue);
			}
			else if (Value is byte[] byteArrayValue)
			{
				writer.WriteLengthEncodedInteger(unchecked((ulong) byteArrayValue.Length));
				writer.Write(byteArrayValue);
			}
			else if (Value is float floatValue)
			{
				writer.Write(BitConverter.GetBytes(floatValue));
			}
			else if (Value is double doubleValue)
			{
				writer.Write(unchecked((ulong) BitConverter.DoubleToInt64Bits(doubleValue)));
			}
			else if (Value is decimal)
			{
				writer.WriteLengthEncodedString("{0}".FormatInvariant(Value));
			}
			else if (Value is MySqlDateTime mySqlDateTimeValue)
			{
				if (mySqlDateTimeValue.IsValidDateTime)
					WriteDateTime(writer, mySqlDateTimeValue.GetDateTime());
				else
					writer.Write((byte) 0);
			}
			else if (Value is DateTime dateTimeValue)
			{
				if ((options & StatementPreparerOptions.DateTimeUtc) != 0 && dateTimeValue.Kind == DateTimeKind.Local)
					throw new MySqlException("DateTime.Kind must not be Local when DateTimeKind setting is Utc (parameter name: {0})".FormatInvariant(ParameterName));
				else if ((options & StatementPreparerOptions.DateTimeLocal) != 0 && dateTimeValue.Kind == DateTimeKind.Utc)
					throw new MySqlException("DateTime.Kind must not be Utc when DateTimeKind setting is Local (parameter name: {0})".FormatInvariant(ParameterName));

				WriteDateTime(writer, dateTimeValue);
			}
			else if (Value is DateTimeOffset dateTimeOffsetValue)
			{
				// store as UTC as it will be read as such when deserialized from a timespan column
				WriteDateTime(writer, dateTimeOffsetValue.UtcDateTime);
			}
			else if (Value is TimeSpan ts)
			{
				WriteTime(writer, ts);
			}
			else if (Value is Guid guidValue)
			{
				StatementPreparerOptions guidOptions = options & StatementPreparerOptions.GuidFormatMask;
				if (guidOptions == StatementPreparerOptions.GuidFormatBinary16 ||
					guidOptions == StatementPreparerOptions.GuidFormatTimeSwapBinary16 ||
					guidOptions == StatementPreparerOptions.GuidFormatLittleEndianBinary16)
				{
					var bytes = guidValue.ToByteArray();
					if (guidOptions != StatementPreparerOptions.GuidFormatLittleEndianBinary16)
					{
						Utility.SwapBytes(bytes, 0, 3);
						Utility.SwapBytes(bytes, 1, 2);
						Utility.SwapBytes(bytes, 4, 5);
						Utility.SwapBytes(bytes, 6, 7);

						if (guidOptions == StatementPreparerOptions.GuidFormatTimeSwapBinary16)
						{
							Utility.SwapBytes(bytes, 0, 4);
							Utility.SwapBytes(bytes, 1, 5);
							Utility.SwapBytes(bytes, 2, 6);
							Utility.SwapBytes(bytes, 3, 7);
							Utility.SwapBytes(bytes, 0, 2);
							Utility.SwapBytes(bytes, 1, 3);
						}
					}
					writer.Write((byte) 16);
					writer.Write(bytes);
				}
				else
				{
					var is32Characters = guidOptions == StatementPreparerOptions.GuidFormatChar32;
					var guidLength = is32Characters ? 32 : 36;
					writer.Write((byte) guidLength);
					var span = writer.GetSpan(guidLength);
					Utf8Formatter.TryFormat(guidValue, span, out _, is32Characters ? 'N' : 'D');
					writer.Advance(guidLength);
				}
			}
			else if (MySqlDbType == MySqlDbType.Int16)
			{
				writer.Write((ushort) (short) Value);
			}
			else if (MySqlDbType == MySqlDbType.UInt16)
			{
				writer.Write((ushort) Value);
			}
			else if (MySqlDbType == MySqlDbType.Int32)
			{
				writer.Write((int) Value);
			}
			else if (MySqlDbType == MySqlDbType.UInt32)
			{
				writer.Write((uint) Value);
			}
			else if (MySqlDbType == MySqlDbType.Int64)
			{
				writer.Write((ulong) (long) Value);
			}
			else if (MySqlDbType == MySqlDbType.UInt64)
			{
				writer.Write((ulong) Value);
			}
			else if ((MySqlDbType == MySqlDbType.String || MySqlDbType == MySqlDbType.VarChar) && HasSetDbType && Value is Enum)
			{
				writer.WriteLengthEncodedString("{0:G}".FormatInvariant(Value));
			}
			else if (Value is Enum)
			{
				writer.Write(Convert.ToInt32(Value));
			}
			else
			{
				throw new NotSupportedException("Parameter type {0} (DbType: {1}) not currently supported. Value: {2}".FormatInvariant(Value.GetType().Name, DbType, Value));
			}
		}

		internal static string NormalizeParameterName(string name)
		{
			name = name.Trim();

			if ((name.StartsWith("@`", StringComparison.Ordinal) || name.StartsWith("?`", StringComparison.Ordinal)) && name.EndsWith("`", StringComparison.Ordinal))
				return name.Substring(2, name.Length - 3).Replace("``", "`");
			if ((name.StartsWith("@'", StringComparison.Ordinal) || name.StartsWith("?'", StringComparison.Ordinal)) && name.EndsWith("'", StringComparison.Ordinal))
				return name.Substring(2, name.Length - 3).Replace("''", "'");
			if ((name.StartsWith("@\"", StringComparison.Ordinal) || name.StartsWith("?\"", StringComparison.Ordinal)) && name.EndsWith("\"", StringComparison.Ordinal))
				return name.Substring(2, name.Length - 3).Replace("\"\"", "\"");

			return name.StartsWith("@", StringComparison.Ordinal) || name.StartsWith("?", StringComparison.Ordinal) ? name.Substring(1) : name;
		}

		private static void WriteDateTime(ByteBufferWriter writer, DateTime dateTime)
		{
			byte length;
			var microseconds = (int) (dateTime.Ticks % 10_000_000) / 10;
			if (microseconds != 0)
				length = 11;
			else if (dateTime.Hour != 0 || dateTime.Minute != 0 || dateTime.Second != 0)
				length = 7;
			else
				length = 4;
			writer.Write(length);
			writer.Write((ushort) dateTime.Year);
			writer.Write((byte) dateTime.Month);
			writer.Write((byte) dateTime.Day);
			if (length > 4)
			{
				writer.Write((byte) dateTime.Hour);
				writer.Write((byte) dateTime.Minute);
				writer.Write((byte) dateTime.Second);
				if (length > 7)
				{
					writer.Write(microseconds);
				}
			}
		}

		private static void WriteTime(ByteBufferWriter writer, TimeSpan timeSpan)
		{
			var ticks = timeSpan.Ticks;
			if (ticks == 0)
			{
				writer.Write((byte) 0);
			}
			else
			{
				if (ticks < 0)
					timeSpan = TimeSpan.FromTicks(-ticks);
				var microseconds = (int) (timeSpan.Ticks % 10_000_000) / 10;
				writer.Write((byte) (microseconds == 0 ? 8 : 12));
				writer.Write((byte) (ticks < 0 ? 1 : 0));
				writer.Write(timeSpan.Days);
				writer.Write((byte) timeSpan.Hours);
				writer.Write((byte) timeSpan.Minutes);
				writer.Write((byte) timeSpan.Seconds);
				if (microseconds != 0)
					writer.Write(microseconds);
			}
		}

		static readonly byte[] s_nullBytes = { 0x4E, 0x55, 0x4C, 0x4C }; // NULL
		static readonly byte[] s_trueBytes = { 0x74, 0x72, 0x75, 0x65 }; // true
		static readonly byte[] s_falseBytes = { 0x66, 0x61, 0x6C, 0x73, 0x65 }; // false
		static readonly byte[] s_binaryBytes = { 0x5F, 0x62, 0x69, 0x6E, 0x61, 0x72, 0x79, 0x27 }; // _binary'

		DbType m_dbType;
		MySqlDbType m_mySqlDbType;
		string? m_name;
		ParameterDirection? m_direction;
		object? m_value;
	}
}
