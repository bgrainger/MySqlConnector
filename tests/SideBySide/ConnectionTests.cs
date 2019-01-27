using System;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;

namespace SideBySide
{
	public class ConnectionTests : IClassFixture<DatabaseFixture>
	{
		public ConnectionTests(DatabaseFixture database)
		{
		}

		[Fact]
		public void GotInfoMessageForNonExistentTable()
		{
			using (var connection = new MySqlConnection(AppConfig.ConnectionString))
			{
				connection.Open();

				var gotEvent = false;
				connection.InfoMessage += (s, a) =>
				{
					gotEvent = true;
					Assert.Single(a.errors);
					Assert.Equal((int) MySqlErrorCode.BadTable, a.errors[0].Code);
				};

				connection.Execute(@"drop table if exists table_does_not_exist;");
				Assert.True(gotEvent);
			}
		}

		[Fact]
		public void NoInfoMessageWhenNotLastStatementInBatch()
		{
			using (var connection = new MySqlConnection(AppConfig.ConnectionString))
			{
				connection.Open();

				var gotEvent = false;
				connection.InfoMessage += (s, a) =>
				{
					gotEvent = true;

					// seeming bug in Connector/NET raises an event with no errors
					Assert.Empty(a.errors);
				};

				connection.Execute(@"drop table if exists table_does_not_exist; select 1;");
#if BASELINE
				Assert.True(gotEvent);
#else
				Assert.False(gotEvent);
#endif
			}
		}

		[Fact]
		public void DefaultConnectionStringIsEmpty()
		{
			using (var connection = new MySqlConnection())
				Assert.Equal("", connection.ConnectionString);
		}

		[Fact]
		public void InitializeWithNullConnectionString()
		{
			using (var connection = new MySqlConnection(default(string)))
				Assert.Equal("", connection.ConnectionString);
		}

		[Fact]
		public void SetConnectionStringToNull()
		{
			using (var connection = new MySqlConnection())
			{
				connection.ConnectionString = null;
				Assert.Equal("", connection.ConnectionString);
			}
		}

		[Fact]
		public void SetConnectionStringToEmptyString()
		{
			using (var connection = new MySqlConnection())
			{
				connection.ConnectionString = "";
				Assert.Equal("", connection.ConnectionString);
			}
		}

		[SkippableFact(Baseline = "Throws NullReferenceException")]
		public void ServerVersionThrows()
		{
			using (var connection = new MySqlConnection())
			{
				Assert.Throws<InvalidOperationException>(() => connection.ServerVersion);
			}
		}

		[SkippableFact(Baseline = "Throws NullReferenceException")]
		public void ServerThreadThrows()
		{
			using (var connection = new MySqlConnection())
			{
				Assert.Throws<InvalidOperationException>(() => connection.ServerThread);
			}
		}

		[Fact]
		public void DatabaseIsEmptyString()
		{
			using (var connection = new MySqlConnection())
			{
				Assert.Equal("", connection.Database);
			}
		}

		[Fact]
		public void DataSourceIsEmptyString()
		{
			using (var connection = new MySqlConnection())
			{
				Assert.Equal("", connection.DataSource);
			}
		}
	}
}
