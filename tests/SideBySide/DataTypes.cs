using System;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;

// mysql-connector-net will throw SqlNullValueException, which is an exception type related to SQL Server:
// "The exception that is thrown when the Value property of a System.Data.SqlTypes structure is set to null."
// However, DbDataReader.GetString etc. are documented as throwing InvalidCastException: https://msdn.microsoft.com/en-us/library/system.data.common.dbdatareader.getstring.aspx
// Additionally, that is what DbDataReader.GetFieldValue<T> throws. For consistency, we prefer InvalidCastException.
#if BASELINE
using GetValueWhenNullException = System.Data.SqlTypes.SqlNullValueException;
using GetGuidWhenNullException = MySql.Data.MySqlClient.MySqlException;
#else
using GetValueWhenNullException = System.InvalidCastException;
using GetGuidWhenNullException = System.InvalidCastException;
#endif

namespace SideBySide
{
	public class DataTypes : IClassFixture<DataTypesFixture>, IDisposable
	{
		public DataTypes(DataTypesFixture database)
		{
			m_database = database;
			m_database.Connection.Open();
		}

		public void Dispose()
		{
			m_database.Connection.Close();
		}

		[Theory]
		[InlineData("SByte", new[] { 1, 0, 0, 0, 0 }, new[] { false, false, true, true, true })]
		[InlineData("Byte", new[] { 1, 0, 0, 0, 0 }, new[] { false, false, false, true, true })]
		[InlineData("Int16", new[] { 1, 0, 0, 0, 0 }, new[] { false, false, true, true, true })]
		[InlineData("UInt16", new[] { 1, 0, 0, 0, 0 }, new[] { false, false, false, true, true })]
		[InlineData("Int24", new[] { 1, 0, 0, 0, 0 }, new[] { false, false, true, true, true })]
		[InlineData("UInt24", new[] { 1, 0, 0, 0, 0 }, new[] { false, false, false, true, true })]
		[InlineData("Int32", new[] { 1, 0, 0, 0, 0 }, new[] { false, false, true, true, true })]
		[InlineData("UInt32", new[] { 1, 0, 0, 0, 0 }, new[] { false, false, false, true, true })]
		[InlineData("Int64", new[] { 1, 0, 0, 0, 0 }, new[] { false, false, true, true, true })]
		[InlineData("UInt64", new[] { 1, 0, 0, 0, 0 }, new[] { false, false, false, true, true })]
		public async Task GetBoolean(string column, int[] flags, bool[] values)
		{
			await DoGetValue(column, (r, n) => r.GetBoolean(n), (r, s) => r.GetBoolean(s), flags, values).ConfigureAwait(false);
		}

		[Theory]
		[InlineData("SByte", new[] { 1, 0, 0, 0, 0 }, new short[] { 0, 0, -128, 127, 123 })]
		[InlineData("Byte", new[] { 1, 0, 0, 0, 0 }, new short[] { 0, 0, 0, 255, 123 })]
		[InlineData("Int16", new[] { 1, 0, 0, 0, 0 }, new short[] { 0, 0, -32768, 32767, 12345 })]
		[InlineData("UInt16", new[] { 1, 0, 0, 2, 0 }, new short[] { 0, 0, 0, 0, 12345 })]
		[InlineData("Int24", new[] { 1, 0, 2, 2, 2 }, new short[] { 0, 0, 0, 0, 0 })]
		[InlineData("UInt24", new[] { 1, 0, 0, 2, 2 }, new short[] { 0, 0, 0, 0, 0 })]
		[InlineData("Int32", new[] { 1, 0, 2, 2, 2 }, new short[] { 0, 0, 0, 0, 0 })]
		[InlineData("UInt32", new[] { 1, 0, 0, 2, 2 }, new short[] { 0, 0, 0, 0, 0 })]
		[InlineData("Int64", new[] { 1, 0, 2, 2, 2 }, new short[] { 0, 0, 0, 0, 0 })]
		[InlineData("UInt64", new[] { 1, 0, 0, 2, 2 }, new short[] { 0, 0, 0, 0, 0 })]
		public async Task GetInt16(string column, int[] flags, short[] values)
		{
			await DoGetValue(column, (r, n) => r.GetInt16(n), (r, s) => r.GetInt16(s), flags, values).ConfigureAwait(false);
		}

		[Theory]
		[InlineData("SByte", new[] { 1, 0, 0, 0, 0 }, new[] { 0, 0, -128, 127, 123 })]
		[InlineData("Byte", new[] { 1, 0, 0, 0, 0 }, new[] { 0, 0, 0, 255, 123 })]
		[InlineData("Int16", new[] { 1, 0, 0, 0, 0 }, new[] { 0, 0, -32768, 32767, 12345 })]
		[InlineData("UInt16", new[] { 1, 0, 0, 0, 0 }, new[] { 0, 0, 0, 65535, 12345 })]
		[InlineData("Int24", new[] { 1, 0, 0, 0, 0 }, new[] { 0, 0, -8388608, 8388607, 1234567 })]
		[InlineData("UInt24", new[] { 1, 0, 0, 0, 0 }, new[] { 0, 0, 0, 16777215, 1234567 })]
		[InlineData("Int32", new[] { 1, 0, 0, 0, 0 }, new[] { 0, 0, -2147483648, 2147483647, 123456789 })]
		[InlineData("UInt32", new[] { 1, 0, 0, 2, 0 }, new[] { 0, 0, 0, 0, 123456789 })]
		[InlineData("Int64", new[] { 1, 0, 2, 2, 2 }, new[] { 0, 0, 0, 0, 0 })]
		[InlineData("UInt64", new[] { 1, 0, 0, 2, 2 }, new[] { 0, 0, 0, 0, 0 })]
		public async Task GetInt32(string column, int[] flags, int[] values)
		{
			await DoGetValue(column, (r, n) => r.GetInt32(n), (r, s) => r.GetInt32(s), flags, values).ConfigureAwait(false);
		}

		[Theory]
		[InlineData("SByte", new[] { 1, 0, 0, 0, 0 }, new[] { 0L, 0, -128, 127, 123 })]
		[InlineData("Byte", new[] { 1, 0, 0, 0, 0 }, new[] { 0L, 0, 0, 255, 123 })]
		[InlineData("Int16", new[] { 1, 0, 0, 0, 0 }, new[] { 0L, 0, -32768, 32767, 12345 })]
		[InlineData("UInt16", new[] { 1, 0, 0, 0, 0 }, new[] { 0L, 0, 0, 65535, 12345 })]
		[InlineData("Int24", new[] { 1, 0, 0, 0, 0 }, new[] { 0L, 0, -8388608, 8388607, 1234567 })]
		[InlineData("UInt24", new[] { 1, 0, 0, 0, 0 }, new[] { 0L, 0, 0, 16777215, 1234567 })]
		[InlineData("Int32", new[] { 1, 0, 0, 0, 0 }, new[] { 0L, 0, -2147483648, 2147483647, 123456789 })]
		[InlineData("UInt32", new[] { 1, 0, 0, 0, 0 }, new[] { 0L, 0, 0, 4294967295L, 123456789 })]
		[InlineData("Int64", new[] { 1, 0, 0, 0, 0 }, new[] { 0L, 0, -9223372036854775808, 9223372036854775807, 1234567890123456789 })]
		[InlineData("UInt64", new[] { 1, 0, 0, 2, 0 }, new[] { 0L, 0, 0, 0, 1234567890123456789 })]
		public async Task GetInt64(string column, int[] flags, long[] values)
		{
			await DoGetValue(column, (r, n) => r.GetInt64(n), (r, s) => r.GetInt64(s), flags, values).ConfigureAwait(false);
		}

		[Theory]
		[InlineData("SByte", new[] { 1, 0, 2, 0, 0 }, new ushort[] { 0, 0, 0, 127, 123 })]
		[InlineData("Byte", new[] { 1, 0, 0, 0, 0 }, new ushort[] { 0, 0, 0, 255, 123 })]
		[InlineData("Int16", new[] { 1, 0, 2, 0, 0 }, new ushort[] { 0, 0, 0, 32767, 12345 })]
		[InlineData("UInt16", new[] { 1, 0, 0, 0, 0 }, new ushort[] { 0, 0, 0, 65535, 12345 })]
		[InlineData("Int24", new[] { 1, 0, 2, 2, 2 }, new ushort[] { 0, 0, 0, 0, 0 })]
		[InlineData("UInt24", new[] { 1, 0, 0, 2, 2 }, new ushort[] { 0, 0, 0, 0, 0 })]
		[InlineData("Int32", new[] { 1, 0, 2, 2, 2 }, new ushort[] { 0, 0, 0, 0, 0 })]
		[InlineData("UInt32", new[] { 1, 0, 0, 2, 2 }, new ushort[] { 0, 0, 0, 0, 0 })]
		[InlineData("Int64", new[] { 1, 0, 2, 2, 2 }, new ushort[] { 0, 0, 0, 0, 0 })]
		[InlineData("UInt64", new[] { 1, 0, 0, 2, 2 }, new ushort[] { 0, 0, 0, 0, 0 })]
		public async Task GetUInt16(string column, int[] flags, ushort[] values)
		{
			await DoGetValue(column, (r, n) => r.GetUInt16(n), (r, s) => r.GetUInt16(s), flags, values).ConfigureAwait(false);
		}

		[Theory]
		[InlineData("SByte", new[] { 1, 0, 2, 0, 0 }, new uint[] { 0, 0, 0, 127, 123 })]
		[InlineData("Byte", new[] { 1, 0, 0, 0, 0 }, new uint[] { 0, 0, 0, 255, 123 })]
		[InlineData("Int16", new[] { 1, 0, 2, 0, 0 }, new uint[] { 0, 0, 0, 32767, 12345 })]
		[InlineData("UInt16", new[] { 1, 0, 0, 0, 0 }, new uint[] { 0, 0, 0, 65535, 12345 })]
		[InlineData("Int24", new[] { 1, 0, 2, 0, 0 }, new uint[] { 0, 0, 0, 8388607, 1234567 })]
		[InlineData("UInt24", new[] { 1, 0, 0, 0, 0 }, new uint[] { 0, 0, 0, 16777215, 1234567 })]
		[InlineData("Int32", new[] { 1, 0, 2, 0, 0 }, new uint[] { 0, 0, 0, 2147483647, 123456789 })]
		[InlineData("UInt32", new[] { 1, 0, 0, 0, 0 }, new uint[] { 0, 0, 0, 4294967295, 123456789 })]
		[InlineData("Int64", new[] { 1, 0, 2, 2, 2 }, new uint[] { 0, 0, 0, 0, 0 })]
		[InlineData("UInt64", new[] { 1, 0, 0, 2, 2 }, new uint[] { 0, 0, 0, 0, 0 })]
		public async Task GetUInt32(string column, int[] flags, uint[] values)
		{
			await DoGetValue(column, (r, n) => r.GetUInt32(n), (r, s) => r.GetUInt32(s), flags, values).ConfigureAwait(false);
		}

		[Theory]
		[InlineData("SByte", new[] { 1, 0, 2, 0, 0 }, new ulong[] { 0, 0, 0, 127, 123 })]
		[InlineData("Byte", new[] { 1, 0, 0, 0, 0 }, new ulong[] { 0, 0, 0, 255, 123 })]
		[InlineData("Int16", new[] { 1, 0, 2, 0, 0 }, new ulong[] { 0, 0, 0, 32767, 12345 })]
		[InlineData("UInt16", new[] { 1, 0, 0, 0, 0 }, new ulong[] { 0, 0, 0, 65535, 12345 })]
		[InlineData("Int24", new[] { 1, 0, 2, 0, 0 }, new ulong[] { 0, 0, 0, 8388607, 1234567 })]
		[InlineData("UInt24", new[] { 1, 0, 0, 0, 0 }, new ulong[] { 0, 0, 0, 16777215, 1234567 })]
		[InlineData("Int32", new[] { 1, 0, 2, 0, 0 }, new ulong[] { 0, 0, 0, 2147483647, 123456789 })]
		[InlineData("UInt32", new[] { 1, 0, 0, 0, 0 }, new ulong[] { 0, 0, 0, 4294967295, 123456789 })]
		[InlineData("Int64", new[] { 1, 0, 2, 0, 0 }, new ulong[] { 0, 0, 0, 9223372036854775807, 1234567890123456789 })]
		[InlineData("UInt64", new[] { 1, 0, 0, 0, 0 }, new ulong[] { 0, 0, 0, 18446744073709551615, 1234567890123456789 })]
		public async Task GetUInt64(string column, int[] flags, ulong[] values)
		{
			await DoGetValue(column, (r, n) => r.GetUInt64(n), (r, s) => r.GetUInt64(s), flags, values).ConfigureAwait(false);
		}

		private async Task DoGetValue<T>(string column, Func<MySqlDataReader, int, T> getInt, Func<MySqlDataReader, string, T> getIntByName, int[] flags, T[] values)
		{
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = $"select {column} from datatypes_integers order by rowid";
				using (var reader = (MySqlDataReader) await cmd.ExecuteReaderAsync().ConfigureAwait(false))
				{
					for (int i = 0; i < flags.Length; i++)
					{
						Assert.True(await reader.ReadAsync().ConfigureAwait(false));
						switch (flags[i])
						{
						case 0: // normal
							Assert.Equal(values[i], getInt(reader, 0));
							Assert.Equal(values[i], getIntByName(reader, column));
							break;

						case 1: // null
							Assert.True(await reader.IsDBNullAsync(0).ConfigureAwait(false));
							break;

						case 2: // overflow
							if (typeof(T) == typeof(int))
							{
								// Utf8Parser code doesn't distinguish between Overflow and FormatException
								Assert.Throws<FormatException>(() => getInt(reader, 0));
								Assert.Throws<FormatException>(() => getIntByName(reader, column));
							}
							else
							{
								Assert.Throws<OverflowException>(() => getInt(reader, 0));
								Assert.Throws<OverflowException>(() => getIntByName(reader, column));
							}
							break;
						}
					}
					Assert.False(await reader.ReadAsync().ConfigureAwait(false));
					Assert.False(await reader.NextResultAsync().ConfigureAwait(false));
				}
			}
		}

		[Theory]
		[InlineData("size", "ENUM", new object[] { null, "small", "medium" })]
		[InlineData("color", "ENUM", new object[] { "red", "orange", "green" })]
		public void QueryEnum(string column, string dataTypeName, object[] expected)
		{
#if BASELINE
			// mysql-connector-net incorrectly returns "VARCHAR" for "ENUM"
			dataTypeName = "VARCHAR";
#endif
			DoQuery("enums", column, dataTypeName, expected, reader => reader.GetString(0));
		}

		[Theory]
		[InlineData("value", "SET", new object[] { null, "", "one", "two", "one,two", "four", "one,four", "two,four", "one,two,four" })]
		public void QuerySet(string column, string dataTypeName, object[] expected)
		{
#if BASELINE
			// mysql-connector-net incorrectly returns "VARCHAR" for "ENUM"
			dataTypeName = "VARCHAR";
#endif
			DoQuery("set", column, dataTypeName, expected, reader => reader.GetString(column));
		}

		[SkippableTheory(Baseline = "https://bugs.mysql.com/bug.php?id=78917")]
		[InlineData("Boolean", "BOOL", new object[] { null, false, true, false, true, true, true })]
		[InlineData("TinyInt1", "BOOL", new object[] { null, false, true, false, true, true, true })]
		public void QueryBoolean(string column, string dataTypeName, object[] expected)
		{
			DoQuery("bools", column, dataTypeName, expected, reader => reader.GetBoolean(0));
		}

		[Theory]
		[InlineData("TinyInt1", "TINYINT", new object[] { null, (sbyte)0, (sbyte)1, (sbyte)0, (sbyte)1, (sbyte)-1, (sbyte)123 })]
		[InlineData("Boolean", "TINYINT", new object[] { null, (sbyte)0, (sbyte)1, (sbyte)0, (sbyte)1, (sbyte)-1, (sbyte)123 })]
		public void QueryTinyIntSbyte(string column, string dataTypeName, object[] expected)
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.TreatTinyAsBoolean = false;
			using (var connection = new MySqlConnection(csb.ConnectionString))
			{
				connection.Open();
				DoQuery("bools", column, dataTypeName, expected, reader => reader.GetSByte(0), baselineCoercedNullValue: default(sbyte), connection: connection);
			}
		}

		[Theory]
		[InlineData("SByte", "TINYINT", new object[] { null, default(sbyte), sbyte.MinValue, sbyte.MaxValue, (sbyte) 123 })]
		public void QuerySByte(string column, string dataTypeName, object[] expected)
		{
			DoQuery("integers", column, dataTypeName, expected, reader => reader.GetSByte(column), baselineCoercedNullValue: default(sbyte));
		}

		[Theory]
		[InlineData("Byte", "TINYINT", new object[] { null, default(byte), byte.MinValue, byte.MaxValue, (byte) 123 })]
		public void QueryByte(string column, string dataTypeName, object[] expected)
		{
			DoQuery("integers", column, dataTypeName, expected, reader => reader.GetByte(0), baselineCoercedNullValue: default(byte));
		}

		[Theory]
		[InlineData("Int16", "SMALLINT", new object[] { null, default(short), short.MinValue, short.MaxValue, (short) 12345 })]
		public void QueryInt16(string column, string dataTypeName, object[] expected)
		{
			DoQuery("integers", column, dataTypeName, expected, reader => reader.GetInt16(0));
		}

		[Theory]
		[InlineData("UInt16", "SMALLINT", new object[] { null, default(ushort), ushort.MinValue, ushort.MaxValue, (ushort) 12345 })]
		public void QueryUInt16(string column, string dataTypeName, object[] expected)
		{
			DoQuery("integers", column, dataTypeName, expected, reader => reader.GetUInt16(column));
		}

		[Theory]
		[InlineData("Int24", "MEDIUMINT", new object[] { null, default(int), -8388608, 8388607, 1234567 })]
		[InlineData("Int32", "INT", new object[] { null, default(int), int.MinValue, int.MaxValue, 123456789 })]
		public void QueryInt32(string column, string dataTypeName, object[] expected)
		{
			DoQuery("integers", column, dataTypeName, expected, reader => reader.GetInt32(0));
		}

		[Theory]
		[InlineData("UInt24", "MEDIUMINT", new object[] { null, default(uint), 0u, 16777215u, 1234567u })]
		[InlineData("UInt32", "INT", new object[] { null, default(uint), uint.MinValue, uint.MaxValue, 123456789u })]
		public void QueryUInt32(string column, string dataTypeName, object[] expected)
		{
#if BASELINE
			// mysql-connector-net incorrectly returns "INT" for "MEDIUMINT UNSIGNED"
			dataTypeName = "INT";
#endif
			DoQuery("integers", column, dataTypeName, expected, reader => reader.GetUInt32(column));
		}

		[Theory]
		[InlineData("Int64", "BIGINT", new object[] { null, default(long), long.MinValue, long.MaxValue, 1234567890123456789 })]
		public void QueryInt64(string column, string dataTypeName, object[] expected)
		{
			DoQuery("integers", column, dataTypeName, expected, reader => reader.GetInt64(0));
		}

		[Theory]
		[InlineData("UInt64", "BIGINT", new object[] { null, default(ulong), ulong.MinValue, ulong.MaxValue, 1234567890123456789u })]
		public void QueryUInt64(string column, string dataTypeName, object[] expected)
		{
			DoQuery("integers", column, dataTypeName, expected, reader => reader.GetUInt64(0));
		}

		[Theory]
		[InlineData("Bit1",  new object[] { null, 0UL, 1UL, 1UL })]
		[InlineData("Bit32",  new object[] { null, 0UL, 1UL, (ulong) uint.MaxValue })]
		[InlineData("Bit64",  new object[] { null, 0UL, 1UL, ulong.MaxValue })]
		public void QueryBits(string column, object[] expected)
		{
			DoQuery("bits", column, "BIT", expected, reader => reader.GetUInt64(column));
		}

		[Theory]
		[InlineData("Single", "FLOAT", new object[] { null, default(float), -3.40282e38f, -1.4013e-45f, 3.40282e38f, 1.4013e-45f })]
		public void QueryFloat(string column, string dataTypeName, object[] expected)
		{
			// don't perform exact queries for floating-point values; they may fail: http://dev.mysql.com/doc/refman/5.7/en/problems-with-float.html
			DoQuery("reals", column, dataTypeName, expected, reader => reader.GetFloat(0), omitWhereTest: true);
		}

		[Theory]
		[InlineData("`Double`", "DOUBLE", new object[] { null, default(double), -1.7976931348623157e308, -5e-324, 1.7976931348623157e308, 5e-324 })]
		public void QueryDouble(string column, string dataTypeName, object[] expected)
		{
			DoQuery("reals", column, dataTypeName, expected, reader => reader.GetDouble(0));
		}

		[Theory]
		[InlineData("SmallDecimal", new object[] { null, "0", "-999.99", "-0.01", "999.99", "0.01" })]
		[InlineData("MediumDecimal", new object[] { null, "0", "-999999999999.99999999", "-0.00000001", "999999999999.99999999", "0.00000001" })]
		// value exceeds the range of a decimal and cannot be deserialized
		// [InlineData("BigDecimal", new object[] { null, "0", "-99999999999999999999.999999999999999999999999999999", "-0.000000000000000000000000000001", "99999999999999999999.999999999999999999999999999999", "0.000000000000000000000000000001" })]
		public void QueryDecimal(string column, object[] expected)
		{
			for (int i = 0; i < expected.Length; i++)
				if (expected[i] != null)
					expected[i] = decimal.Parse((string) expected[i], CultureInfo.InvariantCulture);
			DoQuery("reals", column, "DECIMAL", expected, reader => reader.GetDecimal(0));
		}

		[Theory]
		[InlineData("utf8", new[] { null, "", "ASCII", "Ũńıċōđĕ", c_251ByteString })]
		[InlineData("utf8bin", new[] { null, "", "ASCII", "Ũńıċōđĕ", c_251ByteString })]
		[InlineData("latin1", new[] { null, "", "ASCII", "Lãtïñ", c_251ByteString })]
		[InlineData("latin1bin", new[] { null, "", "ASCII", "Lãtïñ", c_251ByteString })]
		[InlineData("cp1251", new[] { null, "", "ASCII", "АБВГабвг", c_251ByteString })]
		public void QueryString(string column, string[] expected)
		{
			DoQuery("strings", column, "VARCHAR", expected, reader => reader.GetString(0));
		}
		const string c_251ByteString = "This string has exactly 251 characters in it. The encoded length is stored as 0xFC 0xFB 0x00. 0xFB (i.e., 251) is the sentinel byte indicating \"this field is null\". Incorrectly interpreting the (decoded) length as the sentinel byte would corrupt data.";

		[Theory]
		[InlineData("guid", "CHAR(36)", new object[] { null, "00000000-0000-0000-0000-000000000000", "00000000-0000-0000-c000-000000000046", "fd24a0e8-c3f2-4821-a456-35da2dc4bb8f", "6A0E0A40-6228-11D3-A996-0050041896C8" })]
		[InlineData("guidbin", "CHAR(36)", new object[] { null, "00000000-0000-0000-0000-000000000000", "00000000-0000-0000-c000-000000000046", "fd24a0e8-c3f2-4821-a456-35da2dc4bb8f", "6A0E0A40-6228-11D3-A996-0050041896C8" })]
		public void QueryGuid(string column, string dataTypeName, object[] expected)
		{
			for (int i = 0; i < expected.Length; i++)
				if (expected[i] != null)
					expected[i] = Guid.Parse((string) expected[i]);
			DoQuery<GetGuidWhenNullException>("strings", column, dataTypeName, expected, reader => reader.GetGuid(0));
		}

		[Theory]
		[InlineData(false)]
		[InlineData(true)]
		public void QueryBinaryGuid(bool oldGuids)
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.OldGuids = oldGuids;
			using (var connection = new MySqlConnection(csb.ConnectionString))
			{
				connection.Open();
				using (var cmd = connection.CreateCommand())
				{
					cmd.CommandText = @"select guidbin from datatypes_blobs order by rowid;";
					using (var reader = cmd.ExecuteReader())
					{
						Assert.True(reader.Read());
						Assert.Equal(DBNull.Value, reader.GetValue(0));
						Assert.True(reader.Read());
						var expectedBytes = new byte[] { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF };
						var expectedGuid = new Guid(expectedBytes);
						if (oldGuids)
						{
							Assert.Equal(typeof(Guid), reader.GetFieldType(0));
							Assert.Equal(expectedGuid, reader.GetValue(0));
						}
						else
						{
							Assert.Equal(typeof(byte[]), reader.GetFieldType(0));
							Assert.Equal(expectedBytes, reader.GetValue(0));
						}
						Assert.Equal(expectedGuid, reader.GetGuid(0));
						Assert.Equal(expectedBytes, GetBytes(reader));
						Assert.False(reader.Read());
					}

					cmd.CommandText = @"select guidbin from datatypes_strings order by rowid;";
					using (var reader = cmd.ExecuteReader())
					{
						Assert.True(reader.Read());
						Assert.Equal(DBNull.Value, reader.GetValue(0));
						Assert.True(reader.Read());
						if (oldGuids)
						{
							Assert.Equal(typeof(string), reader.GetFieldType(0));
							Assert.Equal("00000000-0000-0000-0000-000000000000", reader.GetValue(0));
							Assert.Equal("00000000-0000-0000-0000-000000000000", reader.GetString("guidbin"));
						}
						else
						{
							Assert.Equal(typeof(Guid), reader.GetFieldType(0));
							Assert.Equal(Guid.Empty, reader.GetValue(0));
						}
						Assert.Equal(Guid.Empty, reader.GetGuid("guidbin"));
					}
				}
			}
		}

		[Theory]
		[InlineData(false)]
		[InlineData(true)]
		public async Task QueryWithGuidParameter(bool oldGuids)
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.OldGuids = oldGuids;
			using (var connection = new MySqlConnection(csb.ConnectionString))
			{
				await connection.OpenAsync().ConfigureAwait(false);
				Assert.Equal(oldGuids? 0L : 1L, (await connection.QueryAsync<long>(@"select count(*) from datatypes_strings where guid = @guid", new { guid = new Guid("fd24a0e8-c3f2-4821-a456-35da2dc4bb8f") }).ConfigureAwait(false)).SingleOrDefault());
				Assert.Equal(oldGuids ? 0L : 1L, (await connection.QueryAsync<long>(@"select count(*) from datatypes_strings where guidbin = @guid", new { guid = new Guid("fd24a0e8-c3f2-4821-a456-35da2dc4bb8f") }).ConfigureAwait(false)).SingleOrDefault());
				Assert.Equal(oldGuids ? 1L : 0L, (await connection.QueryAsync<long>(@"select count(*) from datatypes_blobs where guidbin = @guid", new { guid = new Guid(0x33221100, 0x5544, 0x7766, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF) }).ConfigureAwait(false)).SingleOrDefault());
			}
		}

		[Theory]
		[InlineData("char38", typeof(string))]
		[InlineData("char38bin", typeof(string))]
		[InlineData("text", typeof(string))]
		[InlineData("blob", typeof(byte[]))]
		public async Task GetGuid(string column, Type fieldType)
		{
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = $"select `{column}` from datatypes_guids order by rowid";
				using (var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
				{
					Assert.Equal(fieldType, reader.GetFieldType(0));

					Assert.True(await reader.ReadAsync().ConfigureAwait(false));
					Assert.True(reader.IsDBNull(0));
					Assert.Throws<GetGuidWhenNullException>(() => reader.GetGuid(0));

					Assert.True(await reader.ReadAsync().ConfigureAwait(false));
					Assert.False(reader.IsDBNull(0));
					Assert.NotNull(reader.GetValue(0));
					Assert.IsType(fieldType, reader.GetValue(0));

					Type exceptionType = typeof(GetGuidWhenNullException);
#if BASELINE
					// baseline throws FormatException when conversion from string fails
					if (fieldType == typeof(string))
						exceptionType = typeof(FormatException);
#endif
					Assert.Throws(exceptionType, () => reader.GetGuid(0));

					Assert.True(await reader.ReadAsync().ConfigureAwait(false));
					Assert.NotNull(reader.GetValue(0));
					Assert.IsType(fieldType, reader.GetValue(0));
					Assert.Equal(new Guid("33221100-5544-7766-8899-aabbccddeeff"), reader.GetGuid(0));

					Assert.True(await reader.ReadAsync().ConfigureAwait(false));
					Assert.NotNull(reader.GetValue(0));
					Assert.IsType(fieldType, reader.GetValue(0));
					Assert.Equal(new Guid("33221100-5544-7766-8899-aabbccddeeff"), reader.GetGuid(0));

					Assert.False(await reader.ReadAsync().ConfigureAwait(false));
				}
			}
		}

		[Theory]
		[InlineData("`Date`", "DATE", new object[] { null, "1000 01 01", "9999 12 31", null, "2016 04 05" })]
		[InlineData("`DateTime`", "DATETIME", new object[] { null, "1000 01 01 0 0 0", "9999 12 31 23 59 59 999999", null, "2016 4 5 14 3 4 567890" })]
		[InlineData("`Timestamp`", "TIMESTAMP", new object[] { null, "1970 01 01 0 0 1", "2038 1 18 3 14 7 999999", null, "2016 4 5 14 3 4 567890" })]
		public void QueryDate(string column, string dataTypeName, object[] expected)
		{
			DoQuery("times", column, dataTypeName, ConvertToDateTime(expected), reader => reader.GetDateTime(column.Replace("`", "")));
#if !BASELINE
			DoQuery("times", column, dataTypeName, ConvertToDateTimeOffset(expected), reader => reader.GetDateTimeOffset(0), matchesDefaultType: false);
#endif
		}

		[Theory]
		[InlineData(false)]
		[InlineData(true)]
		public void QueryZeroDateTime(bool convertZeroDateTime)
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.ConvertZeroDateTime = convertZeroDateTime;
			using (var connection = new MySqlConnection(csb.ConnectionString))
			{
				connection.Open();
				using (var cmd = connection.CreateCommand())
				{
					cmd.CommandText = @"select cast(0 as date), cast(0 as datetime);";
					using (var reader = cmd.ExecuteReader() as MySqlDataReader)
					{
						Assert.True(reader.Read());
						if (convertZeroDateTime)
						{
							Assert.Equal(DateTime.MinValue, reader.GetDateTime(0));
							Assert.Equal(DateTime.MinValue, reader.GetDateTime(1));
#if !BASELINE
							Assert.Equal(DateTimeOffset.MinValue, reader.GetDateTimeOffset(0));
							Assert.Equal(DateTimeOffset.MinValue, reader.GetDateTimeOffset(1));
#endif
						}
						else
						{
#if BASELINE
							Assert.Throws<MySql.Data.Types.MySqlConversionException>(() => reader.GetDateTime(0));
							Assert.Throws<MySql.Data.Types.MySqlConversionException>(() => reader.GetDateTime(1));
#else
							Assert.Throws<InvalidCastException>(() => reader.GetDateTime(0));
							Assert.Throws<InvalidCastException>(() => reader.GetDateTime(1));
							Assert.Throws<InvalidCastException>(() => reader.GetDateTimeOffset(0));
							Assert.Throws<InvalidCastException>(() => reader.GetDateTimeOffset(1));
#endif
						}
					}
				}
			}
		}

		[Theory]
		[InlineData("`Time`", "TIME", new object[] { null, "-838 -59 -59", "838 59 59", "0 0 0", "0 14 3 4 567890" })]
		public void QueryTime(string column, string dataTypeName, object[] expected)
		{
			DoQuery("times", column, dataTypeName, ConvertToTimeSpan(expected), reader => reader.GetTimeSpan(0));
		}

		[Theory]
		[InlineData("`Year`", "YEAR", new object[] { null, 1901, 2155, 0, 2016 })]
		public void QueryYear(string column, string dataTypeName, object[] expected)
		{
#if BASELINE
			// mysql-connector-net incorrectly returns "INT" for "YEAR"
			dataTypeName = "INT";
#endif
			DoQuery("times", column, dataTypeName, expected, reader => reader.GetInt32(0));
		}

		[Theory]
		[InlineData("Binary", 100)]
		[InlineData("VarBinary", 0)]
		[InlineData("TinyBlob", 0)]
		[InlineData("Blob", 0)]
		[InlineData("MediumBlob", 0)]
		[InlineData("LongBlob", 0)]
		public void QueryBlob(string column, int padLength)
		{
			var data = new byte[] { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff };
			if (data.Length < padLength)
				Array.Resize(ref data, padLength);

#if BASELINE
			DoQuery<NullReferenceException>("blobs", "`" + column + "`", "BLOB", new object[] { null, data }, GetBytes);
#else
			DoQuery<InvalidCastException>("blobs", "`" + column + "`", "BLOB", new object[] { null, data }, GetBytes);
#endif
		}

		[Theory]
		[InlineData("TinyBlob", 255)]
		[InlineData("Blob", 65535)]
		[InlineData("MediumBlob", 16777215)]
		[InlineData("LongBlob", 33554432)]
		[InlineData("LongBlob", 67108864)]
		public async Task InsertLargeBlobAsync(string column, int size)
		{
			// NOTE: MySQL Server will reset the connection when it receives an oversize packet, so we need to create a test-specific connection here
			using (var connection = new MySqlConnection(AppConfig.CreateConnectionStringBuilder().ConnectionString))
			{
				await connection.OpenAsync();

				var data = CreateByteArray(size);
				var isSupported = size < 1048576 || AppConfig.SupportedFeatures.HasFlag(ServerFeatures.LargePackets);

				long lastInsertId;
				using (var cmd = new MySqlCommand($"insert into datatypes_blobs(`{column}`) values(?)", connection)
				{
					Parameters = { new MySqlParameter { Value = data } }
				})
				{
					try
					{
						await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
						lastInsertId = cmd.LastInsertedId;
						Assert.True(isSupported);
					}
					catch (MySqlException ex)
					{
						lastInsertId = -1;
						Assert.False(isSupported);
						Assert.True(ex.Message.IndexOf("packet") >= 0 || ex.Message.IndexOf("innodb_log_file_size") >= 0);
					}
				}

				if (isSupported)
				{
					var queryResult = (await connection.QueryAsync<byte[]>($"select `{column}` from datatypes_blobs where rowid = {lastInsertId}").ConfigureAwait(false)).Single();
					TestUtilities.AssertEqual(data, queryResult);

					await connection.ExecuteAsync($"delete from datatypes_blobs where rowid = {lastInsertId}").ConfigureAwait(false);
				}
			}
		}

		[Theory]
		[InlineData("TinyBlob", 255)]
		[InlineData("Blob", 65535)]
		[InlineData("MediumBlob", 16777215)]
		[InlineData("LongBlob", 67108864)]
		public void InsertLargeBlobSync(string column, int size)
		{
			// NOTE: MySQL Server will reset the connection when it receives an oversize packet, so we need to create a test-specific connection here
			using (var connection = new MySqlConnection(AppConfig.CreateConnectionStringBuilder().ConnectionString))
			{
				connection.Open();

				var data = CreateByteArray(size);
				var isSupported = size < 1048576 || AppConfig.SupportedFeatures.HasFlag(ServerFeatures.LargePackets);

				long lastInsertId;
				using (var cmd = new MySqlCommand($"insert into datatypes_blobs(`{column}`) values(?)", connection)
				{
					Parameters = { new MySqlParameter { Value = data } }
				})
				{
					try
					{
						cmd.ExecuteNonQuery();
						lastInsertId = cmd.LastInsertedId;
						Assert.True(isSupported);
					}
					catch (MySqlException ex)
					{
						lastInsertId = -1;
						Assert.False(isSupported);
						Assert.True(ex.Message.IndexOf("packet") >= 0 || ex.Message.IndexOf("innodb_log_file_size") >= 0);
					}
				}

				if (isSupported)
				{
					var queryResult = connection.Query<byte[]>($"select `{column}` from datatypes_blobs where rowid = {lastInsertId}").Single();
					TestUtilities.AssertEqual(data, queryResult);

					connection.Execute($"delete from datatypes_blobs where rowid = {lastInsertId}");
				}
			}
		}

#if !NETCOREAPP1_1_2
		[Theory]
		[InlineData("Bit1", "datatypes_bits", MySqlDbType.Bit, 1, typeof(ulong), "N", 0, 0)]
		[InlineData("Bit32", "datatypes_bits", MySqlDbType.Bit, 32, typeof(ulong), "N", 0, 0)]
		[InlineData("Bit64", "datatypes_bits", MySqlDbType.Bit, 64, typeof(ulong), "N", 0, 0)]
		[InlineData("Binary", "datatypes_blobs", MySqlDbType.Binary, 100, typeof(byte[]), "N", 0, 0)]
		[InlineData("VarBinary", "datatypes_blobs", MySqlDbType.VarBinary, 100, typeof(byte[]), "N", 0, 0)]
		[InlineData("TinyBlob", "datatypes_blobs", MySqlDbType.Blob, 255, typeof(byte[]), "N", 0, 0)]
		[InlineData("Blob", "datatypes_blobs", MySqlDbType.Blob, 65535, typeof(byte[]), "LN", 0, 0)]
		[InlineData("MediumBlob", "datatypes_blobs", MySqlDbType.Blob, 16777215, typeof(byte[]), "LN", 0, 0)]
		[InlineData("LongBlob", "datatypes_blobs", MySqlDbType.Blob, int.MaxValue, typeof(byte[]), "LN", 0, 0)]
		[InlineData("guidbin", "datatypes_blobs", MySqlDbType.Binary, 16, typeof(byte[]), "N", 0, 0)]
		[InlineData("rowid", "datatypes_bools", MySqlDbType.Int32, 11, typeof(int), "AK", 0, 0)]
#if BASELINE
		[InlineData("Boolean", "datatypes_bools", MySqlDbType.Byte, 1, typeof(bool), "N", 0, 0)]
		[InlineData("TinyInt1", "datatypes_bools", MySqlDbType.Byte, 1, typeof(bool), "N", 0, 0)]
#else
		[InlineData("Boolean", "datatypes_bools", MySqlDbType.Bool, 1, typeof(bool), "N", 0, 0)]
		[InlineData("TinyInt1", "datatypes_bools", MySqlDbType.Bool, 1, typeof(bool), "N", 0, 0)]
#endif
		[InlineData("size", "datatypes_enums", MySqlDbType.Enum, 7, typeof(string), "N", 0, 0)]
		[InlineData("color", "datatypes_enums", MySqlDbType.Enum, 6, typeof(string), "", 0, 0)]
		[InlineData("char38", "datatypes_guids", MySqlDbType.String, 38, typeof(string), "N", 0, 0)]
		[InlineData("char38bin", "datatypes_guids", MySqlDbType.String, 38, typeof(string), "N", 0, 0)]
		[InlineData("text", "datatypes_guids", MySqlDbType.Text, 65535, typeof(string), "LN", 0, 0)]
		[InlineData("blob", "datatypes_guids", MySqlDbType.Blob, 65535, typeof(byte[]), "LN", 0, 0)]
		[InlineData("SByte", "datatypes_integers", MySqlDbType.Byte, 4, typeof(sbyte), "N", 0, 0)]
		[InlineData("Byte", "datatypes_integers", MySqlDbType.UByte, 3, typeof(byte), "N", 0, 0)]
		[InlineData("Int16", "datatypes_integers", MySqlDbType.Int16, 6, typeof(short), "N", 0, 0)]
		[InlineData("UInt16", "datatypes_integers", MySqlDbType.UInt16, 5, typeof(ushort), "N", 0, 0)]
		[InlineData("Int24", "datatypes_integers", MySqlDbType.Int24, 9, typeof(int), "N", 0, 0)]
		[InlineData("UInt24", "datatypes_integers", MySqlDbType.UInt24, 8, typeof(uint), "N", 0, 0)]
		[InlineData("Int32", "datatypes_integers", MySqlDbType.Int32, 11, typeof(int), "N", 0, 0)]
		[InlineData("UInt32", "datatypes_integers", MySqlDbType.UInt32, 10, typeof(uint), "N", 0, 0)]
		[InlineData("Int64", "datatypes_integers", MySqlDbType.Int64, 20, typeof(long), "N", 0, 0)]
		[InlineData("UInt64", "datatypes_integers", MySqlDbType.UInt64, 20, typeof(ulong), "N", 0, 0)]
		[InlineData("value", "datatypes_json_core", MySqlDbType.JSON, int.MaxValue, typeof(string), "LN", 0, 0)]
		[InlineData("Single", "datatypes_reals", MySqlDbType.Float, 12, typeof(float), "N", 0, 31)]
		[InlineData("Double", "datatypes_reals", MySqlDbType.Double, 22, typeof(double), "N", 0, 31)]
		[InlineData("SmallDecimal", "datatypes_reals", MySqlDbType.NewDecimal, 7, typeof(decimal), "N", 5, 2)]
		[InlineData("MediumDecimal", "datatypes_reals", MySqlDbType.NewDecimal, 30, typeof(decimal), "N", 28, 8)]
		[InlineData("BigDecimal", "datatypes_reals", MySqlDbType.NewDecimal, 52, typeof(decimal), "N", 50, 30)]
		[InlineData("value", "datatypes_set", MySqlDbType.Set, 12, typeof(string), "N", 0, 0)]
		[InlineData("utf8", "datatypes_strings", MySqlDbType.VarChar, 300, typeof(string), "N", 0, 0)]
		[InlineData("utf8bin", "datatypes_strings", MySqlDbType.VarChar, 300, typeof(string), "N", 0, 0)]
		[InlineData("latin1", "datatypes_strings", MySqlDbType.VarChar, 300, typeof(string), "N", 0, 0)]
		[InlineData("latin1bin", "datatypes_strings", MySqlDbType.VarChar, 300, typeof(string), "N", 0, 0)]
		[InlineData("cp1251", "datatypes_strings", MySqlDbType.VarChar, 300, typeof(string), "N", 0, 0)]
		[InlineData("guid", "datatypes_strings", MySqlDbType.Guid, 36, typeof(Guid), "N", 0, 0)]
		[InlineData("guidbin", "datatypes_strings", MySqlDbType.Guid, 36, typeof(Guid), "N", 0, 0)]
		[InlineData("Date", "datatypes_times", MySqlDbType.Date, 10, typeof(DateTime), "N", 0, 0)]
		[InlineData("DateTime", "datatypes_times", MySqlDbType.DateTime, 26, typeof(DateTime), "N", 0, 6)]
		[InlineData("Timestamp", "datatypes_times", MySqlDbType.Timestamp, 26, typeof(DateTime), "N", 0, 6)]
		[InlineData("Time", "datatypes_times", MySqlDbType.Time, 17, typeof(TimeSpan), "N", 0, 6)]
		[InlineData("Year", "datatypes_times", MySqlDbType.Year, 4, typeof(int), "N", 0, 0)]
		public void GetSchemaTable(string column, string table, MySqlDbType mySqlDbType, int columnSize, Type dataType, string flags, int precision, int scale) =>
			DoGetSchemaTable(column, table, mySqlDbType, columnSize, dataType, flags, precision, scale);

		[Theory]
		[InlineData("`decimal-type` decimal(10,0) NOT NULL", "decimal-type", MySqlDbType.NewDecimal, 11, typeof(decimal), "", 10, 0
#if BASELINE
			, Skip = "https://bugs.mysql.com/bug.php?id=88058"
#endif
			)]
		[InlineData("`decimal-type` decimal(10,1) NOT NULL", "decimal-type", MySqlDbType.NewDecimal, 12, typeof(decimal), "", 10, 1)]
		[InlineData("`decimal-type` decimal(10,0) UNSIGNED NOT NULL", "decimal-type", MySqlDbType.NewDecimal, 10, typeof(decimal), "", 10, 0
#if BASELINE
			, Skip = "https://bugs.mysql.com/bug.php?id=88058"
#endif
			)]
		[InlineData("`decimal-type` decimal(10,1) UNSIGNED NOT NULL", "decimal-type", MySqlDbType.NewDecimal, 11, typeof(decimal), "", 10, 1)]
		[InlineData("`decimal-type` decimal(65,30) NOT NULL", "decimal-type", MySqlDbType.NewDecimal, 67, typeof(decimal), "", 65, 30)]
		[InlineData("`decimal-type` decimal(1,1) NOT NULL", "decimal-type", MySqlDbType.NewDecimal, 3, typeof(decimal), "", 1, 1)]
		public void GetSchemaTableForNewColumn(string createColumn, string column, MySqlDbType mySqlDbType, int columnSize, Type dataType, string flags, int precision, int scale)
		{
			m_database.Connection.Execute($@"drop table if exists schema_table;
create table schema_table({createColumn});");

			DoGetSchemaTable(column, "schema_table", mySqlDbType, columnSize, dataType, flags, precision, scale);
		}

		private void DoGetSchemaTable(string column, string table, MySqlDbType mySqlDbType, int columnSize, Type dataType, string flags, int precision, int scale)
		{
			if (table == "datatypes_json_core" && !AppConfig.SupportsJson)
				return;

			var isAutoIncrement = flags.IndexOf('A') != -1;
			var isKey = flags.IndexOf('K') != -1;
			var isLong = flags.IndexOf('L') != -1;
			var allowDbNull = flags.IndexOf('N') != -1;

			using (var command = m_database.Connection.CreateCommand())
			{
				command.CommandText = $"select `{column}` from `{table}`;";
				using (var reader = command.ExecuteReader())
				{
					var schemaTable = reader.GetSchemaTable();
					Assert.Single(schemaTable.Rows);
					var schema = schemaTable.Rows[0];
					Assert.Equal(column, schema["ColumnName"]);
#if BASELINE
					int ordinal = 1; // https://bugs.mysql.com/bug.php?id=61477
#else
					int ordinal = 0;
#endif
					Assert.Equal(ordinal, schema["ColumnOrdinal"]);
					Assert.Equal(dataType, schema["DataType"]);
#if BASELINE
					// https://bugs.mysql.com/bug.php?id=87868, https://bugs.mysql.com/bug.php?id=87876
					if (!column.EndsWith("blob", StringComparison.OrdinalIgnoreCase) && column != "text" && dataType != typeof(Guid) && columnSize != int.MaxValue)
						Assert.Equal(columnSize, schema["ColumnSize"]);
#else
					Assert.Equal(columnSize, schema["ColumnSize"]);
#endif
#if BASELINE
					// https://bugs.mysql.com/bug.php?id=87876
					Assert.Equal(isLong && columnSize != int.MaxValue, schema["IsLong"]);
#else
					Assert.Equal(isLong, schema["IsLong"]);
#endif
					Assert.Equal(isAutoIncrement, schema["IsAutoIncrement"]);
					Assert.Equal(isKey, schema["IsKey"]);
					Assert.Equal(allowDbNull, schema["AllowDBNull"]);
					Assert.Equal(precision, schema["NumericPrecision"]);
					Assert.Equal(scale, schema["NumericScale"]);
#if BASELINE
					if (mySqlDbType == MySqlDbType.Enum || mySqlDbType == MySqlDbType.Set)
						mySqlDbType = MySqlDbType.String;
#endif
					Assert.Equal(mySqlDbType, (MySqlDbType) schema["ProviderType"]);
					Assert.Equal(m_database.Connection.Database, schema["BaseSchemaName"]);
					Assert.Equal(table, schema["BaseTableName"]);
					Assert.Equal(column, schema["BaseColumnName"]);
					Assert.False((bool) schema["IsUnique"]);
					Assert.False((bool) schema["IsRowVersion"]);
					Assert.False((bool) schema["IsReadOnly"]);
				}
			}
		}
#endif

#if !BASELINE
		[Theory]
		[InlineData("Bit1", "datatypes_bits", MySqlDbType.Bit, "BIT", 1, typeof(ulong), "N", -1, 0)]
		[InlineData("Bit32", "datatypes_bits", MySqlDbType.Bit, "BIT", 32, typeof(ulong), "N", -1, 0)]
		[InlineData("Bit64", "datatypes_bits", MySqlDbType.Bit, "BIT", 64, typeof(ulong), "N", -1, 0)]
		[InlineData("Binary", "datatypes_blobs", MySqlDbType.Binary, "BLOB", 100, typeof(byte[]), "N", -1, 0)]
		[InlineData("VarBinary", "datatypes_blobs", MySqlDbType.VarBinary, "BLOB", 100, typeof(byte[]), "N", -1, 0)]
		[InlineData("TinyBlob", "datatypes_blobs", MySqlDbType.Blob, "BLOB", 255, typeof(byte[]), "N", -1, 0)]
		[InlineData("Blob", "datatypes_blobs", MySqlDbType.Blob, "BLOB", 65535, typeof(byte[]), "LN", -1, 0)]
		[InlineData("MediumBlob", "datatypes_blobs", MySqlDbType.Blob, "BLOB", 16777215, typeof(byte[]), "LN", -1, 0)]
		[InlineData("LongBlob", "datatypes_blobs", MySqlDbType.Blob, "BLOB", int.MaxValue, typeof(byte[]), "LN", -1, 0)]
		[InlineData("guidbin", "datatypes_blobs", MySqlDbType.Binary, "BLOB", 16, typeof(byte[]), "N", -1, 0)]
		[InlineData("rowid", "datatypes_bools", MySqlDbType.Int32, "INT", 11, typeof(int), "AK", -1, 0)]
		[InlineData("Boolean", "datatypes_bools", MySqlDbType.Bool, "BOOL", 1, typeof(bool), "N", -1, 0)]
		[InlineData("TinyInt1", "datatypes_bools", MySqlDbType.Bool, "BOOL", 1, typeof(bool), "N", -1, 0)]
		[InlineData("size", "datatypes_enums", MySqlDbType.Enum, "ENUM", 7, typeof(string), "N", -1, 0)]
		[InlineData("color", "datatypes_enums", MySqlDbType.Enum, "ENUM", 6, typeof(string), "", -1, 0)]
		[InlineData("char38", "datatypes_guids", MySqlDbType.String, "CHAR(38)", 38, typeof(string), "N", -1, 0)]
		[InlineData("char38bin", "datatypes_guids", MySqlDbType.String, "CHAR(38)", 38, typeof(string), "N", -1, 0)]
		[InlineData("text", "datatypes_guids", MySqlDbType.Text, "VARCHAR", 65535, typeof(string), "LN", -1, 0)]
		[InlineData("blob", "datatypes_guids", MySqlDbType.Blob, "BLOB", 65535, typeof(byte[]), "LN", -1, 0)]
		[InlineData("SByte", "datatypes_integers", MySqlDbType.Byte, "TINYINT", 4, typeof(sbyte), "N", -1, 0)]
		[InlineData("Byte", "datatypes_integers", MySqlDbType.UByte, "TINYINT", 3, typeof(byte), "N", -1, 0)]
		[InlineData("Int16", "datatypes_integers", MySqlDbType.Int16, "SMALLINT", 6, typeof(short), "N", -1, 0)]
		[InlineData("UInt16", "datatypes_integers", MySqlDbType.UInt16, "SMALLINT", 5, typeof(ushort), "N", -1, 0)]
		[InlineData("Int24", "datatypes_integers", MySqlDbType.Int24, "MEDIUMINT", 9, typeof(int), "N", -1, 0)]
		[InlineData("UInt24", "datatypes_integers", MySqlDbType.UInt24, "MEDIUMINT", 8, typeof(uint), "N", -1, 0)]
		[InlineData("Int32", "datatypes_integers", MySqlDbType.Int32, "INT", 11, typeof(int), "N", -1, 0)]
		[InlineData("UInt32", "datatypes_integers", MySqlDbType.UInt32, "INT", 10, typeof(uint), "N", -1, 0)]
		[InlineData("Int64", "datatypes_integers", MySqlDbType.Int64, "BIGINT", 20, typeof(long), "N", -1, 0)]
		[InlineData("UInt64", "datatypes_integers", MySqlDbType.UInt64, "BIGINT", 20, typeof(ulong), "N", -1, 0)]
		[InlineData("value", "datatypes_json_core", MySqlDbType.JSON, "JSON", int.MaxValue, typeof(string), "LN", -1, 0)]
		[InlineData("Single", "datatypes_reals", MySqlDbType.Float, "FLOAT", 12, typeof(float), "N", -1, 31)]
		[InlineData("Double", "datatypes_reals", MySqlDbType.Double, "DOUBLE", 22, typeof(double), "N", -1, 31)]
		[InlineData("SmallDecimal", "datatypes_reals", MySqlDbType.NewDecimal, "DECIMAL", 7, typeof(decimal), "N", 5, 2)]
		[InlineData("MediumDecimal", "datatypes_reals", MySqlDbType.NewDecimal, "DECIMAL", 30, typeof(decimal), "N", 28, 8)]
		[InlineData("BigDecimal", "datatypes_reals", MySqlDbType.NewDecimal, "DECIMAL", 52, typeof(decimal), "N", 50, 30)]
		[InlineData("value", "datatypes_set", MySqlDbType.Set, "SET", 12, typeof(string), "N", -1, 0)]
		[InlineData("utf8", "datatypes_strings", MySqlDbType.VarChar, "VARCHAR", 300, typeof(string), "N", -1, 0)]
		[InlineData("utf8bin", "datatypes_strings", MySqlDbType.VarChar, "VARCHAR", 300, typeof(string), "N", -1, 0)]
		[InlineData("latin1", "datatypes_strings", MySqlDbType.VarChar, "VARCHAR", 300, typeof(string), "N", -1, 0)]
		[InlineData("latin1bin", "datatypes_strings", MySqlDbType.VarChar, "VARCHAR", 300, typeof(string), "N", -1, 0)]
		[InlineData("cp1251", "datatypes_strings", MySqlDbType.VarChar, "VARCHAR", 300, typeof(string), "N", -1, 0)]
		[InlineData("guid", "datatypes_strings", MySqlDbType.Guid, "CHAR(36)", 36, typeof(Guid), "N", -1, 0)]
		[InlineData("guidbin", "datatypes_strings", MySqlDbType.Guid, "CHAR(36)", 36, typeof(Guid), "N", -1, 0)]
		[InlineData("Date", "datatypes_times", MySqlDbType.Date, "DATE", 10, typeof(DateTime), "N", -1, 0)]
		[InlineData("DateTime", "datatypes_times", MySqlDbType.DateTime, "DATETIME", 26, typeof(DateTime), "N", -1, 6)]
		[InlineData("Timestamp", "datatypes_times", MySqlDbType.Timestamp, "TIMESTAMP", 26, typeof(DateTime), "N", -1, 6)]
		[InlineData("Time", "datatypes_times", MySqlDbType.Time, "TIME", 17, typeof(TimeSpan), "N", -1, 6)]
		[InlineData("Year", "datatypes_times", MySqlDbType.Year, "YEAR", 4, typeof(int), "N", -1, 0)]
		public void GetColumnSchema(string column, string table, MySqlDbType mySqlDbType, string dataTypeName, int columnSize, Type dataType, string flags, int precision, int scale)
		{
			if (table == "datatypes_json_core" && !AppConfig.SupportsJson)
				return;

			var isAutoIncrement = flags.IndexOf('A') != -1;
			var isKey = flags.IndexOf('K') != -1;
			var isLong = flags.IndexOf('L') != -1;
			var allowDbNull = flags.IndexOf('N') != -1;
			var realPrecision = precision == -1 ? default(int?) : precision;

			using (var command = m_database.Connection.CreateCommand())
			{
				command.CommandText = $"select `{column}` from `{table}`;";
				using (var reader = command.ExecuteReader())
				{
					var columns = reader.GetColumnSchema();
					Assert.Single(columns);
					var schema = (MySqlDbColumn) columns[0];
					Assert.Equal(allowDbNull, schema.AllowDBNull);
					Assert.Equal(column, schema.BaseColumnName);
					Assert.Equal(m_database.Connection.Database, schema.BaseSchemaName);
					Assert.Equal(table, schema.BaseTableName);
					Assert.Equal(column, schema.ColumnName);
					Assert.Equal(0, schema.ColumnOrdinal);
					Assert.Equal(dataType, schema.DataType);
					Assert.Equal(dataTypeName, schema.DataTypeName);
					Assert.Equal(columnSize, schema.ColumnSize);
					Assert.False(schema.IsAliased.Value);
					Assert.Equal(isAutoIncrement, schema.IsAutoIncrement);
					Assert.False(schema.IsExpression.Value);
					Assert.False(schema.IsHidden.Value);
					Assert.Equal(isKey, schema.IsKey);
					Assert.Equal(isLong, schema.IsLong);
					Assert.False(schema.IsReadOnly.Value);
					Assert.False(schema.IsUnique.Value);
					Assert.Equal(realPrecision, schema.NumericPrecision);
					Assert.Equal(scale, schema.NumericScale);
					Assert.Equal(mySqlDbType, schema.ProviderType);
				}
			}
		}
#endif

		private static byte[] CreateByteArray(int size)
		{
			var data = new byte[size];
			Random random = new Random(size);
			random.NextBytes(data);

			// ensure each byte value is used at least once
			for (int i = 0; i < Math.Min(255, size); i++)
				data[i] = (byte) i;

			return data;
		}

		[SkippableTheory(ServerFeatures.Json)]
		[InlineData(new object[] { new[] { null, "NULL", "BOOLEAN", "ARRAY", "ARRAY", "ARRAY", "INTEGER", "INTEGER", "OBJECT", "OBJECT" }})]
		public void JsonType(string[] expectedTypes)
		{
			var types = m_database.Connection.Query<string>(@"select JSON_TYPE(value) from datatypes_json_core order by rowid;").ToList();
			Assert.Equal(expectedTypes, types);
		}

		[SkippableTheory(ServerFeatures.Json)]
		[InlineData("value", new[] { null, "null", "true", "[]", "[0]", "[1]", "0", "1", "{}", "{\"a\": \"b\"}" })]
		public void QueryJson(string column, string[] expected)
		{
			string dataTypeName = "JSON";
#if BASELINE
			// mysql-connector-net returns "VARCHAR" for "JSON"
			dataTypeName = "VARCHAR";
#endif
			DoQuery("json_core", column, dataTypeName, expected, reader => reader.GetString(0), omitWhereTest: true);
		}

		private static byte[] GetBytes(DbDataReader reader)
		{
			var size = reader.GetBytes(0, 0, null, 0, 0);
			var result = new byte[size];
			reader.GetBytes(0, 0, result, 0, result.Length);
			return result;
		}

		private void DoQuery(
			string table,
			string column,
			string dataTypeName,
			object[] expected,
			Func<MySqlDataReader, object> getValue,
			object baselineCoercedNullValue = null,
			bool omitWhereTest = false,
			bool matchesDefaultType = true,
			MySqlConnection connection=null)
		{
			DoQuery<GetValueWhenNullException>(table, column, dataTypeName, expected, getValue, baselineCoercedNullValue, omitWhereTest, matchesDefaultType, connection);
		}

		// NOTE: baselineCoercedNullValue is to work around inconsistencies in mysql-connector-net; DBNull.Value will
		// be coerced to 0 by some reader.GetX() methods, but not others.
		private void DoQuery<TException>(
			string table,
			string column,
			string dataTypeName,
			object[] expected,
			Func<MySqlDataReader, object> getValue,
			object baselineCoercedNullValue = null,
			bool omitWhereTest = false,
			bool matchesDefaultType = true,
			MySqlConnection connection=null)
			where TException : Exception
		{
			connection = connection ?? m_database.Connection;
			using (var cmd = connection.CreateCommand())
			{
				cmd.CommandText = $"select {column} from datatypes_{table} order by rowid";
				using (var reader = cmd.ExecuteReader())
				{
					Assert.Equal(dataTypeName, reader.GetDataTypeName(0));
					foreach (var value in expected)
					{
						Assert.True(reader.Read());
						if (value == null)
						{
							Assert.Equal(DBNull.Value, reader.GetValue(0));
#if BASELINE
							if (baselineCoercedNullValue != null)
								Assert.Equal(baselineCoercedNullValue, getValue(reader));
							else
								Assert.Throws<TException>(() => getValue(reader));
#else
							Assert.Throws<TException>(() => getValue(reader));
#endif
						}
						else
						{
							Assert.Equal(value, getValue(reader));

							// test `reader.GetValue` and `reader.GetFieldType` if value matches default type
							if (matchesDefaultType)
							{
								Assert.Equal(value, reader.GetValue(0));
								Assert.Equal(value.GetType(), reader.GetFieldType(0));
							}

							// test `reader.GetFieldValue<value.GetType()>`
							var syncMethod = typeof(MySqlDataReader)
								.GetMethod("GetFieldValue")
								.MakeGenericMethod(value.GetType());
							Assert.Equal(value, syncMethod.Invoke(reader, new object[]{ 0 }));

							// test `reader.GetFieldValueAsync<value.GetType()>`
							var asyncMethod = typeof(MySqlDataReader)
								.GetMethod("GetFieldValueAsync", new []{ typeof(int) })
								.MakeGenericMethod(value.GetType());
							var asyncMethodValue = asyncMethod.Invoke(reader, new object[]{ 0 });
							var asyncMethodGetAwaiter = asyncMethodValue.GetType()
								.GetMethod("GetAwaiter");
							var asyncMethodGetAwaiterValue = asyncMethodGetAwaiter.Invoke(asyncMethodValue, new object[]{ });
							var asyncMethodGetResult = asyncMethodGetAwaiterValue.GetType()
								.GetMethod("GetResult");
							var asyncMethodGetResultValue = asyncMethodGetResult.Invoke(asyncMethodGetAwaiterValue, new object[]{ });
							Assert.Equal(value, asyncMethodGetResultValue);
						}
					}
					Assert.False(reader.Read());
					Assert.False(reader.NextResult());
				}

				if (!omitWhereTest)
				{
					cmd.CommandText = $"select rowid from datatypes_{table} where {column} = @value";
					var p = cmd.CreateParameter();
					p.ParameterName = "@value";
					p.Value = expected.Last();
					cmd.Parameters.Add(p);
					var result = cmd.ExecuteScalar();
					Assert.Equal(Array.IndexOf(expected, p.Value) + 1, result);
				}
			}
		}

		private static object[] ConvertToDateTime(object[] input)
		{
			var output = new object[input.Length];
			for (int i = 0; i < input.Length; i++)
			{
				var value = SplitAndParse(input[i]);
				if (value?.Length == 3)
					output[i] = new DateTime(value[0], value[1], value[2]);
				else if (value?.Length == 6)
					output[i] = new DateTime(value[0], value[1], value[2], value[3], value[4], value[5]);
				else if (value?.Length == 7)
					output[i] = new DateTime(value[0], value[1], value[2], value[3], value[4], value[5], value[6] / 1000).AddTicks(value[6] % 1000 * 10);
			}
			return output;
		}

		private static object[] ConvertToDateTimeOffset(object[] input)
		{
			var output = new object[input.Length];
			var dateTimes = ConvertToDateTime(input);
			for (int i = 0; i < dateTimes.Length; i++)
			{
				if (dateTimes[i] != null)
					output[i] = new DateTimeOffset(DateTime.SpecifyKind((DateTime)dateTimes[i], DateTimeKind.Utc));
			}
			return output;
		}

		private static object[] ConvertToTimeSpan(object[] input)
		{
			var output = new object[input.Length];
			for (int i = 0; i < input.Length; i++)
			{
				var value = SplitAndParse(input[i]);
				if (value?.Length == 3)
					output[i] = new TimeSpan(value[0], value[1], value[2]);
				else if (value?.Length == 5)
					output[i] = new TimeSpan(value[0], value[1], value[2], value[3], value[4] / 1000) + TimeSpan.FromTicks(value[4] % 1000 * 10);
			}
			return output;
		}

		private static int[] SplitAndParse(object obj)
		{
			var value = obj as string;
			if (value == null)
				return null;

			var split = value.Split();
			var output = new int[split.Length];
			for (int i = 0; i < split.Length; i++)
				output[i] = int.Parse(split[i], CultureInfo.InvariantCulture);
			return output;
		}

		readonly DataTypesFixture m_database;
	}
}
