using System;
using System.Collections.Generic;
using System.Text;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;

namespace SideBySide
{
	public class PreparedCommandTests : IClassFixture<DatabaseFixture>
	{
		public PreparedCommandTests(DatabaseFixture database)
		{
		}

		[SkippableFact(Baseline = "Parameter '@data' was not found during prepare.")]
		public void PrepareBeforeBindingParameters()
		{
			using var connection = CreatePrepareConnection();
			connection.Execute($@"DROP TABLE IF EXISTS bind_parameters_test;
CREATE TABLE bind_parameters_test(data TEXT NOT NULL);");

			using (var command = new MySqlCommand(@"INSERT INTO bind_parameters_test(data) VALUES(@data);", connection))
			{
				command.Prepare();
				command.Parameters.AddWithValue("@data", "test");
				command.ExecuteNonQuery();
			}

			Assert.Equal(new[] { "test" }, connection.Query<string>("SELECT data FROM bind_parameters_test;"));
		}

		[SkippableFact(Baseline = "https://bugs.mysql.com/bug.php?id=91753")]
		public void UnnamedParameters()
		{
			using var connection = CreatePrepareConnection();
			connection.Execute($@"DROP TABLE IF EXISTS bind_parameters_test;
CREATE TABLE bind_parameters_test(data1 TEXT NOT NULL, data2 INTEGER);");

			using (var command = new MySqlCommand(@"INSERT INTO bind_parameters_test(data1, data2) VALUES(?, ?);", connection))
			{
				command.Parameters.Add(new MySqlParameter { Value = "test" });
				command.Parameters.Add(new MySqlParameter { Value = 1234 });
				command.Prepare();
				command.ExecuteNonQuery();
			}

			using (var command = new MySqlCommand(@"SELECT data1, data2 FROM bind_parameters_test;", connection))
			{
				command.Prepare();
				using (var reader = command.ExecuteReader())
				{
					Assert.True(reader.Read());
					Assert.Equal("test", reader.GetValue(0));
					Assert.Equal(1234, reader.GetValue(1));
				}
			}
		}

		[Fact]
		public void ReuseCommand()
		{
			using var connection = CreatePrepareConnection();
			connection.Execute($@"DROP TABLE IF EXISTS reuse_command_test;
CREATE TABLE reuse_command_test(rowid INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, data TEXT NOT NULL);");

			using (var command = new MySqlCommand(@"INSERT INTO reuse_command_test(data) VALUES(@data);", connection))
			{
				// work around Connector/NET failure; see PrepareBeforeBindingParameters
				var parameter = command.Parameters.AddWithValue("@data", "");
				command.Prepare();

				foreach (var value in new[] { "one", "two", "three" })
				{
					parameter.Value = value;
					command.ExecuteNonQuery();
				}
			}

			Assert.Equal(new[] { "one", "two", "three" }, connection.Query<string>("SELECT data FROM reuse_command_test ORDER BY rowid;"));
		}

		[Theory]
		[MemberData(nameof(GetInsertAndQueryData))]
		public void InsertAndQuery(bool isPrepared, string dataType, object dataValue)
		{
			var csb = new MySqlConnectionStringBuilder(AppConfig.ConnectionString)
			{
				IgnorePrepare = !isPrepared,
			};
			using var connection = new MySqlConnection(csb.ConnectionString);
			connection.Open();
			connection.Execute($@"DROP TABLE IF EXISTS prepared_command_test;
CREATE TABLE prepared_command_test(rowid INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, data {dataType});");

			using (var command = new MySqlCommand("INSERT INTO prepared_command_test(data) VALUES(@null), (@data);", connection))
			{
				command.Parameters.AddWithValue("@null", null);
				command.Parameters.AddWithValue("@data", dataValue);
				if (isPrepared)
					command.Prepare();
				Assert.Equal(isPrepared, command.IsPrepared);
				command.ExecuteNonQuery();
			}

			using (var command = new MySqlCommand("SELECT data FROM prepared_command_test ORDER BY rowid;", connection))
			{
				if (isPrepared)
					command.Prepare();
				Assert.Equal(isPrepared, command.IsPrepared);

				using var reader = command.ExecuteReader();
				Assert.True(reader.Read());
				Assert.True(reader.IsDBNull(0));

				Assert.True(reader.Read());
				Assert.False(reader.IsDBNull(0));
				Assert.Equal(dataValue, reader.GetValue(0));

				Assert.False(reader.Read());
				Assert.False(reader.NextResult());
			}
		}

		[Fact]
		public void PrepareMultipleTimes()
		{
			using var connection = CreatePrepareConnection();
			using var cmd = new MySqlCommand("SELECT 'test';", connection);
			Assert.False(cmd.IsPrepared);
			cmd.Prepare();
			Assert.True(cmd.IsPrepared);
			cmd.Prepare();
			Assert.Equal("test", cmd.ExecuteScalar());
		}

		[SkippableFact(Baseline = "Connector/NET doesn't cache prepared commands")]
		public void PreparedCommandIsCached()
		{
			using var connection = CreatePrepareConnection();

			using (var cmd = new MySqlCommand("SELECT 'test';", connection))
			{
				cmd.Prepare();
				Assert.Equal("test", cmd.ExecuteScalar());
			}

			using (var cmd = new MySqlCommand("SELECT 'test';", connection))
			{
				Assert.True(cmd.IsPrepared);
				Assert.Equal("test", cmd.ExecuteScalar());
			}
		}

		[Fact]
		public void ThrowsIfNamedParameterUsedButNoParametersDefined()
		{
			using var connection = CreatePrepareConnection();
			using var cmd = new MySqlCommand("SELECT @param;", connection);
#if BASELINE
			Assert.Throws<InvalidOperationException>(() => cmd.Prepare());
#else
			cmd.Prepare();
			Assert.Throws<MySqlException>(() => cmd.ExecuteScalar());
#endif
		}

		[Fact]
		public void ThrowsIfUnnamedParameterUsedButNoParametersDefined()
		{
			using var connection = CreatePrepareConnection();
			using var cmd = new MySqlCommand("SELECT ?;", connection);
			cmd.Prepare();
			Assert.Throws<MySqlException>(() => cmd.ExecuteScalar());
		}

		[Fact]
		public void ThrowsIfUndefinedNamedParameterUsed()
		{
			using var connection = CreatePrepareConnection();
			using var cmd = new MySqlCommand("SELECT @param;", connection);
			cmd.Parameters.AddWithValue("@name", "test");
#if BASELINE
			Assert.Throws<InvalidOperationException>(() => cmd.Prepare());
#else
			cmd.Prepare();
			Assert.Throws<MySqlException>(() => cmd.ExecuteScalar());
#endif
		}

		[Fact]
		public void ThrowsIfTooManyUnnamedParametersUsed()
		{
			using var connection = CreatePrepareConnection();
			using var cmd = new MySqlCommand("SELECT ?, ?;", connection);
			cmd.Parameters.Add(new MySqlParameter { Value = 1 });
			cmd.Prepare();
			Assert.Throws<MySqlException>(() => cmd.ExecuteScalar());
		}

		[Theory]
		[InlineData(1)]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		[InlineData(6)]
		[InlineData(7)]
		[InlineData(8)]
		[InlineData(9)]
		[InlineData(10)]
		[InlineData(11)]
		[InlineData(16)]
		[InlineData(17)]
		[InlineData(100)]
		[InlineData(1000)]
		[InlineData(10000)]
		[InlineData(32767)]
		[InlineData(32768)]
		[InlineData(65535)]
		public void ParametersAreBound(int parameterCount)
		{
			using var connection = CreateConnectionWithTableOfIntegers();
			using var cmd = CreateCommandWithParameters(connection, parameterCount);
			cmd.Prepare();

			using var reader = cmd.ExecuteReader();
			for (var i = 1; i <= Math.Min(parameterCount, 10); i++)
			{
				Assert.True(reader.Read());
				Assert.Equal(i, reader.GetInt32(0));
			}
			Assert.False(reader.Read());
		}

		[Fact]
		public void CannotUse64KParameters()
		{
			using var connection = CreateConnectionWithTableOfIntegers();
			using var cmd = CreateCommandWithParameters(connection, 65536);
			try
			{
				cmd.Prepare();
				Assert.False(true, "Exception wasn't thrown");
			}
			catch (MySqlException ex)
			{
				Assert.Equal(MySqlErrorCode.PreparedStatementManyParameters, (MySqlErrorCode) ex.Number);
			}
		}

		private static MySqlConnection CreateConnectionWithTableOfIntegers()
		{
			var connection = CreatePrepareConnection();
			connection.Execute(@"drop table if exists prepared_command_test; create table prepared_command_test(value int not null); insert into prepared_command_test(value) values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);");
			return connection;
		}

		private static MySqlCommand CreateCommandWithParameters(MySqlConnection connection, int parameterCount)
		{
			var cmd = connection.CreateCommand();
			var sql = new StringBuilder("select value from prepared_command_test where value in (");
			for (int parameterIndex = 1; parameterIndex <= parameterCount; parameterIndex++)
			{
				var parameterName = "p" + parameterIndex;
				cmd.Parameters.AddWithValue(parameterName, parameterIndex);
				if (parameterIndex > 1)
					sql.Append(",");
				sql.Append("@");
				sql.Append(parameterName);
			}
			sql.Append(") order by value;");

			cmd.CommandText = sql.ToString();
			return cmd;
		}

		public static IEnumerable<object[]> GetInsertAndQueryData()
		{
			foreach (var isPrepared in new[] { false, true })
			{
				yield return new object[] { isPrepared, "TINYINT", (sbyte) -123 };
				yield return new object[] { isPrepared, "TINYINT UNSIGNED", (byte) 123 };
				yield return new object[] { isPrepared, "SMALLINT", (short) -12345 };
				yield return new object[] { isPrepared, "SMALLINT UNSIGNED", (ushort) 12345 };
				yield return new object[] { isPrepared, "MEDIUMINT", -1234567 };
				yield return new object[] { isPrepared, "MEDIUMINT UNSIGNED", 1234567u };
				yield return new object[] { isPrepared, "INT", -123456789 };
				yield return new object[] { isPrepared, "INT UNSIGNED", 123456789u };
				yield return new object[] { isPrepared, "BIGINT", -1234567890123456789L };
				yield return new object[] { isPrepared, "BIGINT UNSIGNED", 1234567890123456789UL };
				yield return new object[] { isPrepared, "BIT(10)", 1000UL };
				yield return new object[] { isPrepared, "BINARY(5)", new byte[] { 5, 6, 7, 8, 9 } };
				yield return new object[] { isPrepared, "VARBINARY(100)", new byte[] { 7, 8, 9, 10 } };
				yield return new object[] { isPrepared, "BLOB", new byte[] { 5, 4, 3, 2, 1 } };
				yield return new object[] { isPrepared, "CHAR(36)", new Guid("00112233-4455-6677-8899-AABBCCDDEEFF") };
				yield return new object[] { isPrepared, "FLOAT", 12.375f };
				yield return new object[] { isPrepared, "DOUBLE", 14.21875 };
				yield return new object[] { isPrepared, "DECIMAL(9,3)", 123.45m };
				yield return new object[] { isPrepared, "VARCHAR(100)", "test;@'; -- " };
				yield return new object[] { isPrepared, "TEXT", "testing testing" };
				yield return new object[] { isPrepared, "DATE", new DateTime(2018, 7, 23) };
				yield return new object[] { isPrepared, "DATETIME(3)", new DateTime(2018, 7, 23, 20, 46, 52, 123) };
				yield return new object[] { isPrepared, "ENUM('small', 'medium', 'large')", "medium" };
				yield return new object[] { isPrepared, "SET('one','two','four','eight')", "two,eight" };
				yield return new object[] { isPrepared, "BOOL", true };

#if !BASELINE
				// https://bugs.mysql.com/bug.php?id=91770
				yield return new object[] { isPrepared, "TIME(3)", TimeSpan.Zero.Subtract(new TimeSpan(15, 10, 34, 56, 789)) };

				// https://bugs.mysql.com/bug.php?id=91751
				yield return new object[] { isPrepared, "YEAR", 2134 };
#endif

				if (AppConfig.SupportsJson)
					yield return new object[] { isPrepared, "JSON", "{\"test\": true}" };
			}
		}

		private static MySqlConnection CreatePrepareConnection()
		{
			var csb = new MySqlConnectionStringBuilder(AppConfig.ConnectionString) { IgnorePrepare = false };
			var connection = new MySqlConnection(csb.ConnectionString);
			connection.Open();
			return connection;
		}
	}
}
