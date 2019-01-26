using System;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;

namespace SideBySide
{
	public class UpdateTests : IClassFixture<DatabaseFixture>, IDisposable
	{
		public UpdateTests(DatabaseFixture database)
		{
			m_database = database;
			m_database.Connection.Open();
		}

		public void Dispose()
		{
			m_database.Connection.Close();
		}

		[Theory]
		[InlineData(1, 2)]
		[InlineData(2, 1)]
		[InlineData(3, 0)]
		[InlineData(4, 1)]
		public async Task UpdateRowsExecuteReader(int oldValue, int expectedRowsUpdated)
		{
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = @"drop table if exists update_rows_reader;
create table update_rows_reader(id integer not null primary key auto_increment, value integer not null);
insert into update_rows_reader (value) VALUES (1), (2), (1), (4);
";
				cmd.ExecuteNonQuery();
			}

			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = @"update update_rows_reader set value = @newValue where value = @oldValue";
				var p = cmd.CreateParameter();
				p.ParameterName = "@oldValue";
				p.Value = oldValue;
				cmd.Parameters.Add(p);
				p = cmd.CreateParameter();
				p.ParameterName = "@newValue";
				p.Value = 4;
				cmd.Parameters.Add(p);

				using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
				Assert.False(await reader.ReadAsync().ConfigureAwait(false));
				Assert.Equal(expectedRowsUpdated, reader.RecordsAffected);
				Assert.False(await reader.NextResultAsync().ConfigureAwait(false));
			}
		}

		[Theory]
		[InlineData(1, 2)]
		[InlineData(2, 1)]
		[InlineData(3, 0)]
		[InlineData(4, 1)]
		public async Task UpdateRowsExecuteNonQuery(int oldValue, int expectedRowsUpdated)
		{
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = @"drop table if exists update_rows_non_query;
create table update_rows_non_query(id integer not null primary key auto_increment, value integer not null);
insert into update_rows_non_query (value) VALUES (1), (2), (1), (4);
";
				cmd.ExecuteNonQuery();
			}

			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = @"update update_rows_non_query set value = @newValue where value = @oldValue";
				var p = cmd.CreateParameter();
				p.ParameterName = "@oldValue";
				p.Value = oldValue;
				cmd.Parameters.Add(p);
				p = cmd.CreateParameter();
				p.ParameterName = "@newValue";
				p.Value = 4;
				cmd.Parameters.Add(p);

				var rowsAffected = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
				Assert.Equal(expectedRowsUpdated, rowsAffected);
			}
		}

		[Theory]
		[InlineData(1, 2)]
		[InlineData(2, 1)]
		[InlineData(3, 0)]
		[InlineData(4, 1)]
		public void UpdateRowsDapper(int oldValue, int expectedRowsUpdated)
		{
			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = @"drop table if exists update_rows_dapper;
create table update_rows_dapper(id integer not null primary key auto_increment, value integer not null);
insert into update_rows_dapper (value) VALUES (1), (2), (1), (4);
";
				cmd.ExecuteNonQuery();
			}

			var rowsAffected = m_database.Connection.Execute(@"update update_rows_dapper set value = @newValue where value = @oldValue",
				new { oldValue, newValue = 4 });
			Assert.Equal(expectedRowsUpdated, rowsAffected);
		}

		[Theory]
		[InlineData(true, 1, 2)]
		[InlineData(true, 2, 1)]
		[InlineData(true, 3, 0)]
		[InlineData(true, 4, 0)]
		[InlineData(false, 1, 2)]
		[InlineData(false, 2, 1)]
		[InlineData(false, 3, 0)]
		[InlineData(false, 4, 1)]
		public async Task UpdateRowsDapperAsync(bool useAffectedRows, int oldValue, int expectedRowsUpdated)
		{
			var csb = AppConfig.CreateConnectionStringBuilder();
			csb.UseAffectedRows = useAffectedRows;
			using (var connection = new MySqlConnection(csb.ConnectionString))
			{
				await connection.OpenAsync();
				using (var cmd = connection.CreateCommand())
				{
					cmd.CommandText = @"drop table if exists update_rows_dapper_async;
create table update_rows_dapper_async(id integer not null primary key auto_increment, value integer not null);
insert into update_rows_dapper_async (value) VALUES (1), (2), (1), (4);
";
					cmd.ExecuteNonQuery();
				}
				var rowsAffected = await connection.ExecuteAsync(@"update update_rows_dapper_async set value = @newValue where value = @oldValue",
					new { oldValue, newValue = 4 }).ConfigureAwait(false);
				Assert.Equal(expectedRowsUpdated, rowsAffected);
			}
		}

		[Fact]
		public async Task UpdateDapperNoColumnsWereSelected()
		{
			await m_database.Connection.ExecuteAsync(@"drop table if exists update_station;
create table update_station (
	SID bigint unsigned,
	name text,
	stationType_SID bigint unsigned not null,
	geoPosition_SID bigint unsigned,
	service_start datetime,
	service_end datetime,
	deleted boolean,
	created_on datetime not null,
	externalWebsite text,
	externalTitle text
);
insert into update_station values(1, 'name', 2, null, null, null, false, '2016-09-07 06:28:00', 'https://github.com/mysql-net/MySqlConnector/issues/44', 'Issue #44');
").ConfigureAwait(false);

			var queryString = @"UPDATE update_station SET name=@name,stationType_SID=@stationType_SID,geoPosition_SID=@geoPosition_SID,service_start=@service_start,service_end=@service_end,deleted=@deleted,created_on=@created_on,externalWebsite=@externalWebsite,externalTitle=@externalTitle WHERE SID=@SID";
			var station = new Station
			{
				SID = 1,
				name = "new name",
				stationType_SID = 3,
				geoPosition_SID = null,
				service_start = new DateTime(2016, 1, 1),
				service_end = new DateTime(2017, 12, 31),
				deleted = true,
				created_on = new DateTime(2000, 1, 1),
				externalWebsite = null,
				externalTitle = null,
			};

			try
			{
				await m_database.Connection.QueryAsync<Station>(queryString, station).ConfigureAwait(false);
				Assert.True(false, "Should throw InvalidOperationException");
			}
			catch (InvalidOperationException ex)
			{
				Assert.Equal("No columns were selected", ex.Message);
			}
		}

		public class Station
		{
			public ulong? SID { get; set; }
			public string name { get; set; }
			public ulong stationType_SID { get; set; }
			public ulong? geoPosition_SID { get; set; }
			public DateTime? service_start { get; set; }
			public DateTime? service_end { get; set; }
			public bool? deleted { get; set; }
			public DateTime created_on { get; set; }
			public string externalWebsite { get; set; }
			public string externalTitle { get; set; }
		}

		readonly DatabaseFixture m_database;
	}
}
