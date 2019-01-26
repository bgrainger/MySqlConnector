using System;
using System.Data;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;

namespace SideBySide
{
	public class CommandBuilderTests : IClassFixture<DatabaseFixture>, IDisposable
	{
		public CommandBuilderTests(DatabaseFixture database)
		{
			m_database = database;
			m_database.Connection.Open();
		}

		public void Dispose()
		{
			m_database.Connection.Close();
		}

		[SkippableFact(Baseline = "Throws NullReferenceException")]
		public void DeriveParametersNull()
		{
			Assert.Throws<ArgumentNullException>(() => MySqlCommandBuilder.DeriveParameters(null));
		}

		[SkippableFact(Baseline = "Throws NullReferenceException")]
		public void DeriveParametersNoConnection()
		{
			using var cmd = new MySqlCommand("test");
			cmd.CommandType = CommandType.StoredProcedure;
			Assert.Throws<ArgumentException>(() => MySqlCommandBuilder.DeriveParameters(cmd));
		}

		[SkippableFact(Baseline = "Throws InvalidOperationException")]
		public void DeriveParametersText()
		{
			using var cmd = m_database.Connection.CreateCommand();
			cmd.CommandText = "select 1;";
			Assert.Throws<ArgumentException>(() => MySqlCommandBuilder.DeriveParameters(cmd));
		}

		[SkippableFact(Baseline = "Throws MySqlException")]
		public void DeriveParametersNoCommandText()
		{
			using var cmd = m_database.Connection.CreateCommand();
			cmd.CommandType = CommandType.StoredProcedure;
			Assert.Throws<ArgumentException>(() => MySqlCommandBuilder.DeriveParameters(cmd));
		}

#if !NETCOREAPP1_1_2
		[Fact]
		public void Insert()
		{
			m_database.Connection.Execute(@"drop table if exists command_builder_insert;
create table command_builder_insert
(
	id int not null primary key,
	value varchar(100)
);");
			using (var dataAdapter = new MySqlDataAdapter("select * from command_builder_insert", m_database.Connection))
			using (new MySqlCommandBuilder(dataAdapter))
			{
				var dataTable = new DataTable();
				dataAdapter.Fill(dataTable);

				var row = dataTable.NewRow();
				row["id"] = 1;
				row["value"] = "inserted";
				dataTable.Rows.Add(row);

				dataAdapter.Update(dataTable);
			}

			Assert.Equal(1, m_database.Connection.ExecuteScalar<int>("select count(*) from command_builder_insert;"));
			Assert.Equal("inserted", m_database.Connection.ExecuteScalar<string>("select value from command_builder_insert where id = 1;"));
		}

		[Fact]
		public void Update()
		{
			m_database.Connection.Execute(@"drop table if exists command_builder_update;
create table command_builder_update
(
	id int not null primary key,
	value varchar(100)
);
insert into command_builder_update values(1, 'one'), (2, 'two');
");
			using (var dataAdapter = new MySqlDataAdapter("select * from command_builder_update", m_database.Connection))
			using (new MySqlCommandBuilder(dataAdapter))
			{
				var dataTable = new DataTable();
				dataAdapter.Fill(dataTable);

				dataTable.Rows[0]["value"] = "updated";
				dataAdapter.Update(dataTable);
			}

			Assert.Equal("updated", m_database.Connection.ExecuteScalar<string>("select value from command_builder_update where id = 1;"));
		}

		[Fact]
		public void Delete()
		{
			m_database.Connection.Execute(@"drop table if exists command_builder_delete;
create table command_builder_delete
(
	id int not null primary key,
	value varchar(100)
);
insert into command_builder_delete values(1, 'one'), (2, 'two');
");
			using (var dataAdapter = new MySqlDataAdapter("select * from command_builder_delete", m_database.Connection))
			using (new MySqlCommandBuilder(dataAdapter))
			{
				var dataTable = new DataTable();
				dataAdapter.Fill(dataTable);

				dataTable.Rows[0].Delete();

				dataAdapter.Update(dataTable);
			}

			Assert.Equal(1, m_database.Connection.ExecuteScalar<int>("select count(*) from command_builder_delete;"));
		}

		[Theory]
		[InlineData("test", "`test`")]
		[InlineData("te`st", "`te``st`")]
		[InlineData("`test`", "```test```"
#if BASELINE
			, Skip = "Doesn't quote leading quotes"
#endif
		)]
		public void QuoteIdentifier(string input, string expected)
		{
			var cb = new MySqlCommandBuilder();
			Assert.Equal(expected, cb.QuoteIdentifier(input));
		}

		[Theory]
		[InlineData("test", "test")]
		[InlineData("`test`", "test")]
		[InlineData("`te``st`", "te`st")]
		[InlineData("```test```", "`test`")]
		public void UnquoteIdentifier(string input, string expected)
		{
			var cb = new MySqlCommandBuilder();
			Assert.Equal(expected, cb.UnquoteIdentifier(input));
		}
#endif

		readonly DatabaseFixture m_database;
	}
}
