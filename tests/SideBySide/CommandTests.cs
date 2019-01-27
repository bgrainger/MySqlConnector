using System;
using System.Data;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;

namespace SideBySide
{
	public class CommandTests : IClassFixture<DatabaseFixture>
	{
		public CommandTests(DatabaseFixture database)
		{
			m_database = database;
		}

		[Fact]
		public void CreateCommandSetsConnection()
		{
			using var command = m_database.Connection.CreateCommand();
			Assert.Equal(m_database.Connection, command.Connection);
		}

		[Fact]
		public void CreateCommandDoesNotSetTransaction()
		{
			using var connection = new MySqlConnection(AppConfig.ConnectionString);
			connection.Open();
			using var transaction = connection.BeginTransaction();
			using var cmd = connection.CreateCommand();
			Assert.Null(cmd.Transaction);
		}

		[Fact]
		public void ExecuteReaderRequiresConnection()
		{
			using var command = new MySqlCommand();
			Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());
		}

		[Fact]
		public void ExecuteReaderRequiresOpenConnection()
		{
			using var connection = new MySqlConnection();
			using var command = connection.CreateCommand();
			Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());
		}

		[Fact]
		public void PrepareRequiresConnection()
		{
			using var command = new MySqlCommand();
			Assert.Throws<InvalidOperationException>(() => command.Prepare());
		}

		[Fact]
		public void PrepareRequiresOpenConnection()
		{
			using var connection = new MySqlConnection();
			using var command = connection.CreateCommand();
			Assert.Throws<InvalidOperationException>(() => command.Prepare());
		}

		[Fact]
		public void NewCommandIsNotPrepared()
		{
			using (var command = new MySqlCommand())
				Assert.False(command.IsPrepared);
		}

		[Fact]
		public void CommandWithoutConnectionIsNotPrepared()
		{
			using (var command = new MySqlCommand())
			{
				command.CommandText = "SELECT 1";
				Assert.False(command.IsPrepared);
			}
		}

		[Fact]
		public void CommandWithClosedConnectionIsNotPrepared()
		{
			using (var connection = new MySqlConnection())
			using (var command = connection.CreateCommand())
			{
				command.CommandText = "SELECT 1";
				Assert.False(command.IsPrepared);
			}
		}

		[Fact]
		public void ExecuteNonQueryForSelectReturnsNegativeOne()
		{
			using var connection = new MySqlConnection(m_database.Connection.ConnectionString);
			using var command = connection.CreateCommand();
			connection.Open();
			command.CommandText = "SELECT 1;";
			Assert.Equal(-1, command.ExecuteNonQuery());
		}

		[Fact]
		public async Task ExecuteNonQueryReturnValue()
		{
			using var connection = new MySqlConnection(m_database.Connection.ConnectionString);
			await connection.OpenAsync();
			await connection.ExecuteAsync(@"drop table if exists execute_non_query;
create table execute_non_query(id integer not null primary key auto_increment, value text null);");
			Assert.Equal(4, await connection.ExecuteAsync("insert into execute_non_query(value) values(null), (null), ('one'), ('two');"));
			Assert.Equal(-1, await connection.ExecuteAsync("select value from execute_non_query;"));
			Assert.Equal(2, await connection.ExecuteAsync("delete from execute_non_query where value is null;"));
			Assert.Equal(1, await connection.ExecuteAsync("update execute_non_query set value = 'three' where value = 'one';"));
		}

		[SkippableFact(Baseline = "https://bugs.mysql.com/bug.php?id=88611")]
		public void CommandTransactionMustBeSet()
		{
			using var connection = new MySqlConnection(AppConfig.ConnectionString);
			connection.Open();
			using var transaction = connection.BeginTransaction();
			using var command = connection.CreateCommand();
			command.CommandText = "SELECT 1;";
			Assert.Throws<InvalidOperationException>(() => command.ExecuteScalar());

			command.Transaction = transaction;
			TestUtilities.AssertIsOne(command.ExecuteScalar());
		}

		[Fact]
		public void IgnoreCommandTransactionIgnoresNull()
		{
			using var connection = new MySqlConnection(GetIgnoreCommandTransactionConnectionString());
			connection.Open();
			using var _ = connection.BeginTransaction();
			using var command = connection.CreateCommand();
			command.CommandText = "SELECT 1;";
			TestUtilities.AssertIsOne(command.ExecuteScalar());
		}

		[Fact]
		public void IgnoreCommandTransactionIgnoresDisposedTransaction()
		{
			using var connection = new MySqlConnection(GetIgnoreCommandTransactionConnectionString());
			connection.Open();

			var transaction = connection.BeginTransaction();
			transaction.Commit();
			transaction.Dispose();

			using var command = connection.CreateCommand();
			command.CommandText = "SELECT 1;";
			command.Transaction = transaction;
			TestUtilities.AssertIsOne(command.ExecuteScalar());
		}

		[Fact]
		public void IgnoreCommandTransactionIgnoresDifferentTransaction()
		{
			using var connection1 = new MySqlConnection(AppConfig.ConnectionString);
			using var connection2 = new MySqlConnection(GetIgnoreCommandTransactionConnectionString());
			connection1.Open();
			connection2.Open();

			using var transaction1 = connection1.BeginTransaction();
			using var command2 = connection2.CreateCommand();
			command2.Transaction = transaction1;
			command2.CommandText = "SELECT 1;";
			TestUtilities.AssertIsOne(command2.ExecuteScalar());
		}

		[Fact]
		public void ThrowsIfNamedParameterUsedButNoParametersDefined()
		{
			using var connection = new MySqlConnection(AppConfig.ConnectionString);
			connection.Open();
			using var cmd = new MySqlCommand("SELECT @param;", connection);
			Assert.Throws<MySqlException>(() => cmd.ExecuteScalar());
		}

		[Fact]
		public void ThrowsIfUnnamedParameterUsedButNoParametersDefined()
		{
			using var connection = new MySqlConnection(AppConfig.ConnectionString);
			connection.Open();
			using var cmd = new MySqlCommand("SELECT ?;", connection);
#if BASELINE
			Assert.Throws<IndexOutOfRangeException>(() => cmd.ExecuteScalar());
#else
			Assert.Throws<MySqlException>(() => cmd.ExecuteScalar());
#endif
		}

		[Fact]
		public void ThrowsIfUndefinedNamedParameterUsed()
		{
			using var connection = new MySqlConnection(AppConfig.ConnectionString);
			connection.Open();
			using var cmd = new MySqlCommand("SELECT @param;", connection);
			cmd.Parameters.AddWithValue("@name", "test");
			Assert.Throws<MySqlException>(() => cmd.ExecuteScalar());
		}

		[Fact]
		public void ThrowsIfTooManyUnnamedParametersUsed()
		{
			using var connection = new MySqlConnection(AppConfig.ConnectionString);
			connection.Open();
			using var cmd = new MySqlCommand("SELECT ?, ?;", connection);
			cmd.Parameters.Add(new MySqlParameter { Value = 1 });
#if BASELINE
			Assert.Throws<IndexOutOfRangeException>(() => cmd.ExecuteScalar());
#else
			Assert.Throws<MySqlException>(() => cmd.ExecuteScalar());
#endif
		}

		[Fact]
		public void CloneCommand()
		{
			using var connection = new MySqlConnection(AppConfig.ConnectionString);
			connection.Open();
			using var transaction = connection.BeginTransaction();
			var param = new MySqlParameter("@param", MySqlDbType.Decimal) { Value = 12.3m };
			using var cmd = new MySqlCommand("SELECT @param;", connection, transaction)
			{
				CommandType = CommandType.StoredProcedure,
				Parameters = { param },
			};
			using var cmd2 = (MySqlCommand) cmd.Clone();

			Assert.Equal(cmd.Connection, cmd2.Connection);
			Assert.Equal(cmd.Transaction, cmd2.Transaction);
			Assert.Equal(cmd.CommandText, cmd2.CommandText);
			Assert.Equal(cmd.CommandType, cmd2.CommandType);
			Assert.Single(cmd2.Parameters);

			var param2 = cmd2.Parameters[0];
			Assert.Equal(param.ParameterName, param2.ParameterName);
			Assert.Equal(param.MySqlDbType, param2.MySqlDbType);
			Assert.Equal(param.Value, param2.Value);

			cmd.CommandText = "New text";
			Assert.NotEqual(cmd.CommandText, cmd2.CommandText);

			param.Value = 0m;
			Assert.NotEqual(0m, cmd2.Parameters[0].Value);
		}

		[SkippableFact(Baseline = "https://bugs.mysql.com/bug.php?id=94075")]
		public void CancelEmptyCommandIsNoop()
		{
			using var cmd = new MySqlCommand();
			cmd.Cancel();
		}

		private static string GetIgnoreCommandTransactionConnectionString()
		{
#if BASELINE
			return AppConfig.ConnectionString;
#else
			return new MySqlConnectionStringBuilder(AppConfig.ConnectionString)
			{
				IgnoreCommandTransaction = true
			}.ConnectionString;
#endif
		}

		readonly DatabaseFixture m_database;
	}
}
