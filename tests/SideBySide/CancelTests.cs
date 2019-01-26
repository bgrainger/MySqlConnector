using System;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;

namespace SideBySide
{
	public class CancelTests : IClassFixture<CancelFixture>, IDisposable
	{
		public CancelTests(CancelFixture database) => m_database = database;

		public void Dispose()
		{
			// after cancelling, the connection should still be usable
			Assert.Equal(ConnectionState.Open, m_database.Connection.State);
			using var cmd = new MySqlCommand(@"select sum(value) from integers;", m_database.Connection);
			Assert.Equal(210m, cmd.ExecuteScalar());
		}

		[Fact]
		public void NoCancel()
		{
			using var cmd = new MySqlCommand("SELECT SLEEP(0.25)", m_database.Connection);
			var stopwatch = Stopwatch.StartNew();
			Assert.Equal(0L, Convert.ToInt64(cmd.ExecuteScalar()));
			Assert.InRange(stopwatch.ElapsedMilliseconds, 100, 1000);
		}

		[SkippableFact(ServerFeatures.Timeout)]
		public void CancelCommand()
		{
			using var cmd = new MySqlCommand("SELECT SLEEP(5)", m_database.Connection);
			var task = Task.Run(async () =>
			{
				await Task.Delay(TimeSpan.FromSeconds(0.5));
				cmd.Cancel();
			});

			var stopwatch = Stopwatch.StartNew();
			TestUtilities.AssertIsOne(cmd.ExecuteScalar());
			Assert.InRange(stopwatch.ElapsedMilliseconds, 250, 2500);

			task.Wait(); // shouldn't throw
		}

		[SkippableFact(ServerFeatures.Timeout)]
		public void CancelReaderAsynchronously()
		{
			using var barrier = new Barrier(2);
			using var cmd = new MySqlCommand(c_hugeQuery, m_database.Connection);
			var task = Task.Run(() =>
			{
				barrier.SignalAndWait();
				cmd.Cancel();
			});

			int rows = 0;
			using (var reader = cmd.ExecuteReader())
			{
				Assert.True(reader.Read());

				barrier.SignalAndWait();
				try
				{
					while (reader.Read())
						rows++;
				}
				catch (MySqlException ex)
				{
					Assert.Equal((int) MySqlErrorCode.QueryInterrupted, ex.Number);
				}

				// query returns 25 billion rows; we shouldn't have read many of them
				Assert.InRange(rows, 0, 10000000);
			}

			task.Wait(); // shouldn't throw
		}

		[SkippableFact(ServerFeatures.Timeout)]
		public void CancelCommandBeforeRead()
		{
			using var cmd = new MySqlCommand(c_hugeQuery, m_database.Connection);
			using var reader = cmd.ExecuteReader();
			cmd.Cancel();

			var stopwatch = Stopwatch.StartNew();
			int rows = 0;
			try
			{
				while (reader.Read())
					rows++;
			}
			catch (MySqlException ex)
			{
				Assert.Equal((int) MySqlErrorCode.QueryInterrupted, ex.Number);
			}
			Assert.False(reader.NextResult());
			TestUtilities.AssertDuration(stopwatch, 0, 1000);
			Assert.InRange(rows, 0, 10000000);
		}

		[SkippableFact(ServerFeatures.Timeout, Baseline = "Hangs in NextResult")]
		public void CancelMultiStatementReader()
		{
			using var barrier = new Barrier(2);
			using var cmd = new MySqlCommand(c_hugeQuery + c_hugeQuery + c_hugeQuery, m_database.Connection);
			var task = Task.Run(() =>
			{
				barrier.SignalAndWait();
				cmd.Cancel();
			});

			int rows = 0;
			using (var reader = cmd.ExecuteReader())
			{
				Assert.True(reader.Read());

				barrier.SignalAndWait();
				try
				{
					while (reader.Read())
						rows++;
				}
				catch (MySqlException ex)
				{
					Assert.Equal((int) MySqlErrorCode.QueryInterrupted, ex.Number);
				}

				// query returns 25 billion rows; we shouldn't have read many of them
				Assert.InRange(rows, 0, 10000000);

				Assert.False(reader.NextResult());
			}

			task.Wait(); // shouldn't throw
		}

		[SkippableFact(ServerFeatures.Timeout)]
		public void DapperQueryMultiple()
		{
			Stopwatch stopwatch;
			using (var gridReader = m_database.Connection.QueryMultiple(@"select 1; " + c_hugeQuery))
			{
				var first = gridReader.Read<int>().ToList();
				Assert.Single(first);
				Assert.Equal(1, first[0]);

				// don't read the second result set; disposing the GridReader should Cancel the command
				stopwatch = Stopwatch.StartNew();
			}
			stopwatch.Stop();
			TestUtilities.AssertDuration(stopwatch, 0, 1000);
		}

#if !BASELINE
		[SkippableFact(ServerFeatures.Timeout)]
		public async Task CancelCommandWithTokenBeforeExecuteScalar()
		{
			using var cmd = new MySqlCommand("select 1;", m_database.Connection);
			try
			{
				await cmd.ExecuteScalarAsync(s_canceledToken);
				Assert.True(false);
			}
			catch (OperationCanceledException ex)
			{
				Assert.Equal(s_canceledToken, ex.CancellationToken);
			}
		}

		[SkippableFact(ServerFeatures.Timeout)]
		public async Task CancelCommandWithTokenBeforeExecuteNonQuery()
		{
			using var cmd = new MySqlCommand("select 1;", m_database.Connection);
			try
			{
				await cmd.ExecuteNonQueryAsync(s_canceledToken);
				Assert.True(false);
			}
			catch (OperationCanceledException ex)
			{
				Assert.Equal(s_canceledToken, ex.CancellationToken);
			}
		}

		[SkippableFact(ServerFeatures.Timeout)]
		public async Task CancelCommandWithTokenBeforeExecuteReader()
		{
			using var cmd = new MySqlCommand("select 1;", m_database.Connection);
			try
			{
				await cmd.ExecuteReaderAsync(s_canceledToken);
				Assert.True(false);
			}
			catch (OperationCanceledException ex)
			{
				Assert.Equal(s_canceledToken, ex.CancellationToken);
			}
		}

		[Fact]
		public async Task CancelCompletedCommand()
		{
			await m_database.Connection.ExecuteAsync(@"drop table if exists cancel_completed_command;
create table cancel_completed_command (
	id bigint unsigned,
	value varchar(45)
);").ConfigureAwait(false);

			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = @"insert into cancel_completed_command (id, value) values (1, null);";

				using (await cmd.ExecuteReaderAsync().ConfigureAwait(false))
					cmd.Cancel();
			}

			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = @"update cancel_completed_command SET value = ""value"" where id = 1;";

				await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
			}

			using (var cmd = m_database.Connection.CreateCommand())
			{
				cmd.CommandText = "select value from cancel_completed_command where id = 1;";
				var value = (string) await cmd.ExecuteScalarAsync();
				Assert.Equal("value", value);
			}
		}

		[Fact]
		public void ImplicitCancelWithDapper()
		{
			m_database.Connection.Execute(@"drop table if exists cancel_completed_command;
create table cancel_completed_command(id integer not null primary key, value text null);");

			// a query that returns 0 fields will cause Dapper to cancel the command
			m_database.Connection.Query<int>("insert into cancel_completed_command(id, value) values (1, null);");

			m_database.Connection.Execute("update cancel_completed_command set value = 'value' where id = 1;");

			var value = m_database.Connection.Query<string>(@"select value from cancel_completed_command where id = 1").FirstOrDefault();
			Assert.Equal("value", value);
		}

		[SkippableFact(ServerFeatures.Timeout)]
		public async Task CancelHugeQueryWithTokenAfterExecuteReader()
		{
			using var cmd = new MySqlCommand(c_hugeQuery, m_database.Connection);
			using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(0.5));
			using var reader = await cmd.ExecuteReaderAsync(cts.Token);
			var rows = 0;
			try
			{
				while (await reader.ReadAsync(cts.Token))
					rows++;
				Assert.True(false);
			}
			catch (OperationCanceledException ex)
			{
				Assert.Equal(cts.Token, ex.CancellationToken);
				Assert.InRange(rows, 0, 10000000);
			}

			// no more result sets
			Assert.False(reader.Read());
			Assert.False(reader.NextResult());
		}

		[SkippableFact(ServerFeatures.Timeout)]
		public async Task CancelHugeQueryWithTokenInNextResult()
		{
			using var cmd = new MySqlCommand(c_hugeQuery + "select 1, 2, 3;", m_database.Connection);
			using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(0.5));
			using var reader = await cmd.ExecuteReaderAsync(cts.Token);

			// read first result set
			Assert.True(await reader.ReadAsync(cts.Token));

			try
			{
				// skip to the next result set
				Assert.True(await reader.NextResultAsync(cts.Token));

				// shouldn't get here
				Assert.True(false);
			}
			catch (OperationCanceledException ex)
			{
				Assert.Equal(cts.Token, ex.CancellationToken);
			}

			// no more result sets
			Assert.False(reader.Read());
			Assert.False(reader.NextResult());
		}

		[SkippableFact(ServerFeatures.Timeout)]
		public async Task CancelSlowQueryWithTokenAfterExecuteReader()
		{
			using var cmd = new MySqlCommand(c_slowQuery, m_database.Connection);
			using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(0.5));

			// the call to ExecuteReader should block until the token is cancelled
			var stopwatch = Stopwatch.StartNew();
			using var reader = await cmd.ExecuteReaderAsync(cts.Token);
			TestUtilities.AssertDuration(stopwatch, 450, 3000);

			var rows = 0;
			try
			{
				// the OperationCanceledException is thrown later, from ReadAsync
				while (await reader.ReadAsync(cts.Token))
					rows++;
				Assert.True(false);
			}
			catch (OperationCanceledException ex)
			{
				Assert.Equal(cts.Token, ex.CancellationToken);
				Assert.InRange(rows, 0, 100);
			}
		}

		[SkippableFact(ServerFeatures.Timeout)]
		public async Task CancelSlowQueryWithTokenAfterNextResult()
		{
			using var cmd = new MySqlCommand("SELECT 1; " + c_slowQuery, m_database.Connection);
			using var reader = await cmd.ExecuteReaderAsync();

			// first resultset should be available immediately
			Assert.True(reader.Read());
			Assert.Equal(1, reader.GetInt32(0));
			Assert.False(reader.Read());

			using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(0.5)))
			{
				// the call to NextResult should block until the token is cancelled
				var stopwatch = Stopwatch.StartNew();
				Assert.True(await reader.NextResultAsync(cts.Token));
				TestUtilities.AssertDuration(stopwatch, 450, 1500);

				int rows = 0;
				try
				{
					// the OperationCanceledException is thrown later, from ReadAsync
					while (await reader.ReadAsync(cts.Token))
						rows++;
				}
				catch (OperationCanceledException ex)
				{
					Assert.Equal(cts.Token, ex.CancellationToken);
					Assert.InRange(rows, 0, 100);
				}
			}

			Assert.False(await reader.NextResultAsync());
		}

		[SkippableFact(ServerFeatures.Timeout)]
		public async Task CancelMultiStatementInRead()
		{
			using var cmd = new MySqlCommand(c_hugeQuery + c_hugeQuery + c_hugeQuery, m_database.Connection);
			using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(0.5));
			using var reader = await cmd.ExecuteReaderAsync();
			var rows = 0;
			try
			{
				while (await reader.ReadAsync(cts.Token))
					rows++;

				Assert.True(false);
			}
			catch (OperationCanceledException ex)
			{
				Assert.Equal(cts.Token, ex.CancellationToken);
				Assert.InRange(rows, 0, 10000000);
			}

			// no more result sets; the whole command was cancelled
			Assert.False(reader.Read());
			Assert.False(reader.NextResult());
		}

		private static CancellationToken GetCanceledToken()
		{
			var cts = new CancellationTokenSource();
			cts.Cancel();
			return cts.Token;
		}

		static readonly CancellationToken s_canceledToken = GetCanceledToken();
#endif

		// returns billions of rows
		const string c_hugeQuery = @"select * from integers a join integers b join integers c join integers d join integers e join integers f join integers g join integers h;";

		// takes a long time to return any rows
		const string c_slowQuery = @"select * from integers a join integers b join integers c join integers d join integers e join integers f join integers g join integers h
where sqrt(a.value) + sqrt(b.value) + sqrt(c.value) + sqrt(d.value) + sqrt(e.value) + sqrt(f.value) + sqrt(g.value) + sqrt(h.value) = 20;";

		readonly DatabaseFixture m_database;
	}
}
