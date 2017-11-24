using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;

namespace SideBySide
{
	public class StoredProcedureTests : IClassFixture<StoredProcedureFixture>
	{
		public StoredProcedureTests(StoredProcedureFixture database)
		{
			m_database = database;
		}

		[Theory]
		[InlineData("FUNCTION", "NonQuery")]
		[InlineData("FUNCTION", "Scalar")]
		[InlineData("FUNCTION", "Reader")]
		[InlineData("PROCEDURE", "NonQuery")]
		[InlineData("PROCEDURE", "Scalar")]
		[InlineData("PROCEDURE", "Reader")]
		public async Task StoredProcedureEcho(string procedureType, string executorType)
		{
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = "echo" + (procedureType == "FUNCTION" ? "f" : "p");
				cmd.CommandType = CommandType.StoredProcedure;

				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@name",
					DbType = DbType.String,
					Direction = ParameterDirection.Input,
					Value = "hello",
				});

				// we make the assumption that Stored Procedures with ParameterDirection.ReturnValue are functions
				if (procedureType == "FUNCTION")
				{
					cmd.Parameters.Add(new MySqlParameter
					{
						ParameterName = "@result",
						DbType = DbType.String,
						Direction = ParameterDirection.ReturnValue,
					});
				}

				var result = await ExecuteCommandAsync(cmd, executorType);
				if (procedureType == "PROCEDURE" && executorType != "NonQuery")
					Assert.Equal(cmd.Parameters["@name"].Value, result);
				if (procedureType == "FUNCTION")
					Assert.Equal(cmd.Parameters["@name"].Value, cmd.Parameters["@result"].Value);
			}
		}

		[Fact]
		public void CallFailingFunction()
		{
			using (var connection = new MySqlConnection(AppConfig.ConnectionString))
			using (var command = connection.CreateCommand())
			{
				connection.Open();

				command.CommandType = CommandType.StoredProcedure;
				command.CommandText = "failing_function";

				var returnParameter = command.CreateParameter();
				returnParameter.DbType = DbType.Int32;
				returnParameter.Direction = ParameterDirection.ReturnValue;
				command.Parameters.Add(returnParameter);

				Assert.Throws<MySqlException>(() => command.ExecuteNonQuery());
			}
		}

		[Fact]
		public void CallFailingFunctionInTransaction()
		{
			using (var connection = new MySqlConnection(AppConfig.ConnectionString))
			{
				connection.Open();
				using (var transaction = connection.BeginTransaction())
				using (var command = connection.CreateCommand())
				{
					command.Transaction = transaction;
					command.CommandType = CommandType.StoredProcedure;
					command.CommandText = "failing_function";

					var returnParameter = command.CreateParameter();
					returnParameter.DbType = DbType.Int32;
					returnParameter.Direction = ParameterDirection.ReturnValue;
					command.Parameters.Add(returnParameter);

					Assert.Throws<MySqlException>(() => command.ExecuteNonQuery());
					transaction.Commit();
				}
			}
		}

		[SkippableTheory(ServerFeatures.StoredProcedures)]
		[InlineData("FUNCTION")]
		[InlineData("PROCEDURE")]
		public async Task StoredProcedureEchoException(string procedureType)
		{
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = "echo" + (procedureType == "FUNCTION" ? "f" : "p");
				cmd.CommandType = CommandType.StoredProcedure;

				if (procedureType == "FUNCTION")
					await Assert.ThrowsAsync<InvalidOperationException>(async () => await cmd.ExecuteNonQueryAsync());
				else
					await Assert.ThrowsAsync<ArgumentException>(async () => await cmd.ExecuteNonQueryAsync());
			}
		}

		[Theory]
		[InlineData("NonQuery")]
		[InlineData("Scalar")]
		[InlineData("Reader")]
		public async Task StoredProcedureCircle(string executorType)
		{
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = "circle";
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@radius",
					DbType = DbType.Double,
					Direction = ParameterDirection.Input,
					Value = 1.0,
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@height",
					DbType = DbType.Double,
					Direction = ParameterDirection.Input,
					Value = 2.0,
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@name",
					DbType = DbType.String,
					Direction = ParameterDirection.Input,
					Value = "awesome",
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@diameter",
					DbType = DbType.Double,
					Direction = ParameterDirection.Output,
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@circumference",
					DbType = DbType.Double,
					Direction = ParameterDirection.Output,
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@area",
					DbType = DbType.Double,
					Direction = ParameterDirection.Output,
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@volume",
					DbType = DbType.Double,
					Direction = ParameterDirection.Output,
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@shape",
					DbType = DbType.String,
					Direction = ParameterDirection.Output,
				});

				await CircleAssertions(cmd, executorType);
			}
		}

		[SkippableTheory(ServerFeatures.StoredProcedures)]
		[InlineData("NonQuery")]
		[InlineData("Scalar")]
		[InlineData("Reader")]
		public async Task StoredProcedureCircleCached(string executorType)
		{
			// reorder parameters
			// remove return types
			// remove directions (MySqlConnector only, MySql.Data does not fix these up)
			// CachedProcedure class should fix everything up based on parameter names
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = "circle";
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@name",
					Value = "awesome",
#if BASELINE
					Direction = ParameterDirection.Input,
#endif
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@radius",
					Value = 1.0,
#if BASELINE
					Direction = ParameterDirection.Input,
#endif
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@shape",
#if BASELINE
					Direction = ParameterDirection.Output,
#endif
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@height",
					Value = 2.0,
#if BASELINE
					Direction = ParameterDirection.Input,
#endif
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@diameter",
#if BASELINE
					Direction = ParameterDirection.Output,
#endif
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@area",
#if BASELINE
					Direction = ParameterDirection.Output,
#endif
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@volume",
#if BASELINE
					Direction = ParameterDirection.Output,
#endif
				});
				cmd.Parameters.Add(new MySqlParameter
				{
					ParameterName = "@circumference",
#if BASELINE
					Direction = ParameterDirection.Output,
#endif
				});

				await CircleAssertions(cmd, executorType);
			}
		}

		private async Task CircleAssertions(DbCommand cmd, string executorType)
		{
			var result = await ExecuteCommandAsync(cmd, executorType);
			if (executorType != "NonQuery")
				Assert.Equal((string)cmd.Parameters["@name"].Value + (string)cmd.Parameters["@shape"].Value, result);

			Assert.Equal(2 * (double)cmd.Parameters["@radius"].Value, cmd.Parameters["@diameter"].Value);
			Assert.Equal(Math.PI * (double)cmd.Parameters["@radius"].Value, cmd.Parameters["@area"].Value);
			Assert.Equal(Math.PI * Math.Pow((double)cmd.Parameters["@radius"].Value, 2), cmd.Parameters["@area"].Value);
			Assert.Equal((double)cmd.Parameters["@area"].Value * (double)cmd.Parameters["@height"].Value, cmd.Parameters["@volume"].Value);
		}

		private async Task<object> ExecuteCommandAsync(DbCommand cmd, string executorType)
		{
			switch (executorType)
			{
				case "NonQuery":
					await cmd.ExecuteNonQueryAsync();
					return null;
				case "Scalar":
					return await cmd.ExecuteScalarAsync();
				default:
					using (var reader = await cmd.ExecuteReaderAsync())
					{
						if (await reader.ReadAsync())
							return reader.GetValue(0);
						return null;
					}
			}
		}

		[Theory]
		[InlineData("factor")]
		[InlineData("@factor")]
		[InlineData("?factor")]
		public async Task MultipleRows(string paramaterName)
		{
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = "number_multiples";
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.Add(new MySqlParameter { ParameterName = paramaterName, Value = 3 });
				using (var reader = await cmd.ExecuteReaderAsync())
				{
					Assert.True(await reader.ReadAsync());
					Assert.Equal("six", reader.GetString(0));
					Assert.True(await reader.ReadAsync());
					Assert.Equal("three", reader.GetString(0));
					Assert.False(await reader.ReadAsync());
					Assert.False(await reader.NextResultAsync());
				}
			}
		}

		[Fact]
		public async Task MultipleResultSets()
		{
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = "multiple_result_sets";
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.Add(new MySqlParameter { ParameterName = "@pivot", Value = 4 });
				using (var reader = await cmd.ExecuteReaderAsync())
				{
					Assert.True(await reader.ReadAsync());
					Assert.Equal("one", reader.GetString(0));
					Assert.True(await reader.ReadAsync());
					Assert.Equal("three", reader.GetString(0));
					Assert.True(await reader.ReadAsync());
					Assert.Equal("two", reader.GetString(0));
					Assert.False(await reader.ReadAsync());
					Assert.True(await reader.NextResultAsync());

					Assert.True(await reader.ReadAsync());
					Assert.Equal("eight", reader.GetString(0));
					Assert.True(await reader.ReadAsync());
					Assert.Equal("five", reader.GetString(0));
					Assert.True(await reader.ReadAsync());
					Assert.Equal("seven", reader.GetString(0));
					Assert.True(await reader.ReadAsync());
					Assert.Equal("six", reader.GetString(0));
					Assert.False(await reader.ReadAsync());
					Assert.False(await reader.NextResultAsync());
				}
			}
		}

		[Fact]
		public async Task InOut()
		{
			var parameter = new MySqlParameter
			{
				ParameterName = "high",
				DbType = DbType.Int32,
				Direction = ParameterDirection.InputOutput,
				Value = 1
			};
			while ((int) parameter.Value < 8)
			{
				using (var cmd = m_database.Connection.CreateCommand())
				{
					var nextValue = (int) parameter.Value + 1;
					cmd.CommandText = "number_lister";
					cmd.CommandType = CommandType.StoredProcedure;
					cmd.Parameters.Add(parameter);
					cmd.Prepare();
					using (var reader = await cmd.ExecuteReaderAsync())
					{
						for (var i = 0; i < (int) parameter.Value; i++)
						{
							Assert.True(await reader.ReadAsync());
							Assert.Equal(i + 1, reader.GetInt32(0));
							Assert.True(reader.GetString(1).Length > 0);
						}
						await reader.NextResultAsync();
					}
					Assert.Equal(nextValue, parameter.Value);
				}
			}
		}

		[SkippableTheory(Baseline = "https://bugs.mysql.com/bug.php?id=84220")]
		[InlineData(false)]
		[InlineData(true)]
		public async Task DottedName(bool useDatabaseName)
		{
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = (useDatabaseName ? $"{m_database.Connection.Database}." : "") + "`dotted.name`";
				cmd.CommandType = CommandType.StoredProcedure;
				using (var reader = await cmd.ExecuteReaderAsync())
				{
					Assert.True(await reader.ReadAsync());
					Assert.Equal(1, reader.GetInt32(0));
					Assert.Equal(2, reader.GetInt32(1));
					Assert.Equal(3, reader.GetInt32(2));
					Assert.False(await reader.ReadAsync());
					Assert.False(await reader.NextResultAsync());
				}
			}
		}

#if !NETCOREAPP1_1_2
		[Theory]
		[InlineData("echof", "FUNCTION", "varchar(63)", "BEGIN RETURN name; END", "NO", "CONTAINS SQL")]
		[InlineData("echop", "PROCEDURE", null, "BEGIN SELECT name; END", "NO", "CONTAINS SQL")]
		[InlineData("failing_function", "FUNCTION", "int(11)", "BEGIN DECLARE v1 INT; SELECT c1 FROM table_that_does_not_exist INTO v1; RETURN v1; END", "NO", "CONTAINS SQL")]
		public void ProceduresSchema(string procedureName, string procedureType, string dtdIdentifier, string routineDefinition, string isDeterministic, string dataAccess)
		{
			var dataTable = m_database.Connection.GetSchema("Procedures");
			var schema = m_database.Connection.Database;
			var row = dataTable.Rows.Cast<DataRow>().Single(x => schema.Equals(x["ROUTINE_SCHEMA"]) && procedureName.Equals(x["ROUTINE_NAME"]));

			Assert.Equal(procedureName, row["SPECIFIC_NAME"]);
			Assert.Equal(procedureType, row["ROUTINE_TYPE"]);
			if (dtdIdentifier == null)
				Assert.Equal(DBNull.Value, row["DTD_IDENTIFIER"]);
			else
				Assert.Equal(dtdIdentifier, ((string) row["DTD_IDENTIFIER"]).Split(' ')[0]);
			Assert.Equal(routineDefinition, NormalizeSpaces((string) row["ROUTINE_DEFINITION"]));
			Assert.Equal(isDeterministic, row["IS_DETERMINISTIC"]);
			Assert.Equal(dataAccess, ((string) row["SQL_DATA_ACCESS"]).Replace('_', ' '));
		}
#endif

		[Fact]
		public void RowCountInTransaction()
		{
			m_database.Connection.Execute(@"
DROP PROCEDURE IF EXISTS servicetickets_update;
DROP TABLE IF EXISTS servicetickets_tracking;
DROP TABLE IF EXISTS servicetickets;

CREATE TABLE servicetickets
(
	ServiceTicketID VARCHAR(36) NOT NULL PRIMARY KEY,
	Title LONGTEXT
);

CREATE TABLE servicetickets_tracking
(
	ServiceTicketID VARCHAR(36) NOT NULL PRIMARY KEY,
	timestamp BIGINT
);

INSERT INTO servicetickets values('04d07d89-3f32-449e-8879-c017ef75d950', 'A');
INSERT INTO servicetickets_tracking values('04d07d89-3f32-449e-8879-c017ef75d950', 0);

CREATE PROCEDURE `servicetickets_update`(

	inServiceTicketID VARCHAR(36),
	inTitle LONGTEXT
)
BEGIN
UPDATE `servicetickets`
SET  `Title` = inTitle
WHERE `ServiceTicketID` = inServiceTicketID;

/* Since the update 'could' potentially returns 0 as row affected count when we make a double update with the same values, to be sure, make a fake update on metadatas time column */
UPDATE `servicetickets_tracking` 
SET `timestamp` = ROUND(UNIX_TIMESTAMP(CURTIME(4)) * 1000)
WHERE `ServiceTicketID` = inServiceTicketID;

END;");
			for (int i = 0; i < 5; i++)
			{
				using (var connection = new MySqlConnection(AppConfig.ConnectionString))
				{
					connection.Open();

					using (var t = connection.BeginTransaction(IsolationLevel.ReadCommitted))
					{

						string procName = "servicetickets_update";
						using (var cmd = connection.CreateCommand())
						{
							cmd.CommandText = procName;
							cmd.CommandType = CommandType.StoredProcedure;
							cmd.Transaction = t;

							MySqlParameter p = new MySqlParameter();
							p.ParameterName = "inServiceTicketID";
							p.MySqlDbType = MySqlDbType.Guid;
							p.Value = new Guid("04d07d89-3f32-449e-8879-c017ef75d950");
							cmd.Parameters.Add(p);

							p = new MySqlParameter();
							p.ParameterName = "inTitle";
							p.MySqlDbType = MySqlDbType.String;
							p.Value = "A";
							cmd.Parameters.Add(p);

							var rowCount = cmd.ExecuteNonQuery();
							Assert.Equal(1, rowCount);

							t.Commit();
						}
					}
				}
			}
		}

		private static string NormalizeSpaces(string input)
		{
			input = input.Replace('\r', ' ');
			input = input.Replace('\n', ' ');
			input = input.Replace('\t', ' ');
			int startingLength;
			do
			{
				startingLength = input.Length;
				input = input.Replace("  ", " ");
			} while (input.Length != startingLength);
			return input;
		}

		readonly DatabaseFixture m_database;
	}
}
