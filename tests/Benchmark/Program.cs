using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.CsProj;
using BenchmarkDotNet.Validators;
using MySql.Data.MySqlClient;
using MySqlConnector.Direct;
using MySqlConnector.Logging;
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Payloads;

namespace Benchmark
{
	class Program
	{
		static async Task Main()
		{
#if false
			var customConfig = ManualConfig
				.Create(DefaultConfig.Instance)
				.With(JitOptimizationsValidator.FailOnError)
				.With(MemoryDiagnoser.Default)
				.With(StatisticColumn.AllStatistics)
				.With(Job.Default.With(Runtime.Clr).With(Jit.RyuJit).With(Platform.X64).With(CsProjClassicNetToolchain.Net47).WithId("net47"))
				// .With(Job.Default.With(Runtime.Core).With(CsProjCoreToolchain.NetCoreApp11).WithId("netcore11"))
				.With(Job.Default.With(Runtime.Core).With(CsProjCoreToolchain.NetCoreApp20).WithId("netcore20"))
				.With(DefaultExporters.Csv);

			var summary = BenchmarkRunner.Run<RandomHelloWorld>(customConfig);
			Console.WriteLine(summary);
#else
			var session = new MySqlSession("localhost", 3306, "benchmarkdbuser", "benchmarkdbpass", "hello_world");
			var isConnected = await session.ConnectAsync();

#if true
			var rhw = new Fortunes();
			await rhw.LoadFortunesRows();

			// MySqlConnectorLogManager.Provider = new ConsoleLoggerProvider();
			Console.Write("starting test");
			// Console.ReadLine();

			var sw = Stopwatch.StartNew();
			var tasks = Enumerable.Range(0, concurrencyLevel).Select(x => Task.Run(async () => await RunTestDirectAsync())).ToArray();
			await Task.WhenAll(tasks);
#else
			var sw = Stopwatch.StartNew();
			/*for (int i = 0; i < startingRequestCount; i++)
				session.Ping();*/
			await session.ExecuteAsync("SELECT * FROM fortune;");
			while (await session.ReadAsync())
			{
				var id = session.ReadInt32();
				var message = session.ReadString();
				Console.WriteLine($"{id}\t{message}");
			}
#endif
			sw.Stop();
			Console.WriteLine("done in {0}, {1:f1} req/s", sw.Elapsed, startingRequestCount / sw.Elapsed.TotalSeconds);
			Console.Write("Press ENTER");
			// Console.ReadLine();
			await MySqlConnection.ClearAllPoolsAsync();
#endif
		}
		const int concurrencyLevel = 80;
		const int startingRequestCount = 1000000;
		private static int requestCount = startingRequestCount;


		static async Task RunTestAsync()
		{
			var rhw = new Fortunes();

			const int batchSize = 100;
			while (true)
			{
				int result = Interlocked.Add(ref requestCount, -batchSize);
				if (result >= 0)
				{
					for (int i = 0; i < batchSize; i++)
					{
						await rhw.LoadFortunesRows();
					}
				}
				else
				{
					break;
				}
			}
		}

		static async Task RunTestDirectAsync()
		{
			var session = new MySqlSession("localhost", 3306, "benchmarkdbuser", "benchmarkdbpass", "hello_world");
			await session.ConnectAsync();

			const int batchSize = 100;
			while (true)
			{
				int result = Interlocked.Add(ref requestCount, -batchSize);
				if (result >= 0)
				{
					for (int i = 0; i < batchSize; i++)
					{
						await session.ExecuteAsync("SELECT * FROM fortune;");
						while (await session.ReadAsync())
						{
							session.ReadInt32();
							session.ReadString();
						}
					}
				}
				else
				{
					break;
				}
			}
		}
	}

	public class StateChanged
	{
		[Benchmark(Baseline = true)]
		public StateChangeEventArgs Old()
		{
			return new StateChangeEventArgs(previousState, newState);
		}

		[Benchmark]
		public StateChangeEventArgs New()
	{
		return
			previousState == ConnectionState.Closed && newState == ConnectionState.Connecting ? s_stateChangeClosedConnecting :
			previousState == ConnectionState.Connecting && newState == ConnectionState.Open ? s_stateChangeConnectingOpen :
			previousState == ConnectionState.Open && newState == ConnectionState.Closed ? s_stateChangeOpenClosed :
			new StateChangeEventArgs(previousState, newState);
		}

		static readonly StateChangeEventArgs s_stateChangeClosedConnecting = new StateChangeEventArgs(ConnectionState.Closed, ConnectionState.Connecting);
		static readonly StateChangeEventArgs s_stateChangeConnectingOpen = new StateChangeEventArgs(ConnectionState.Connecting, ConnectionState.Open);
		static readonly StateChangeEventArgs s_stateChangeOpenClosed = new StateChangeEventArgs(ConnectionState.Open, ConnectionState.Closed);

		ConnectionState previousState = ConnectionState.Connecting;
		ConnectionState newState = ConnectionState.Open;
	}

	public class World
	{
		public int Id { get; set; }

		public int RandomNumber { get; set; }
	}

	public interface IRandom
	{
		int Next(int minValue, int maxValue);
	}

	public class DefaultRandom : IRandom
	{
		private static int nextSeed = 0;
		// Random isn't thread safe
		private static readonly ThreadLocal<Random> _random = new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref nextSeed)));

		public int Next(int minValue, int maxValue)
		{
			return _random.Value.Next(minValue, maxValue);
		}
	}

	public class RandomHelloWorld
	{
		public RandomHelloWorld()
		{
			_random = new DefaultRandom();
		}

		[Benchmark]
		public async Task<World> LoadSingleQueryRow()
		{
			using (var db = new MySqlConnection())
			{
				db.ConnectionString = connectionString;
				await db.OpenAsync();

				using (var cmd = CreateReadCommand(db))
					return await ReadSingleRow(cmd);
			}
		}

		async Task<World> ReadSingleRow(DbCommand cmd)
		{
			using (var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow))
			{
				await rdr.ReadAsync();

				return new World
				{
					Id = rdr.GetInt32(0),
					RandomNumber = rdr.GetInt32(1)
				};
			}
		}

		DbCommand CreateReadCommand(DbConnection connection)
		{
			var cmd = connection.CreateCommand();
			cmd.CommandText = "SELECT id, randomNumber FROM world WHERE Id = @Id";
			var id = cmd.CreateParameter();
			id.ParameterName = "@Id";
			id.DbType = DbType.Int32;
			id.Value = _random.Next(1, 10001);
			cmd.Parameters.Add(id);

			return cmd;
		}

		IRandom _random;
		string connectionString = "server=127.0.0.1;port=1000000;user id=mysqltest;password='test;key=\"val';database=hello_world;ssl mode=none;Default Command Timeout=0;Connection Timeout=0;Connection Reset=false";
		// string connectionString = "server=dlo-dev-mysql02;user id=benchmarkdbuser;password=benchmarkdbpass;database=hello_world;ssl mode=none;Default Command Timeout=0;Connection Timeout=0;Connection Reset=false";
	}

	public class Fortune : IComparable<Fortune>, IComparable
	{
		public int Id { get; set; }

		public int _Id { get; set; }

		public string Message { get; set; }

		public int CompareTo(object obj)
		{
			return CompareTo((Fortune) obj);
		}

		public int CompareTo(Fortune other)
		{
			return String.CompareOrdinal(Message, other.Message);
		}
	}

	public class Fortunes
	{
		public async Task<IEnumerable<Fortune>> LoadFortunesRows()
		{
			var result = new List<Fortune>();

			using (var db = new MySqlConnection())
			using (var cmd = db.CreateCommand())
			{
				cmd.CommandText = "SELECT id, message FROM fortune";

				db.ConnectionString = _connectionString;
				await db.OpenAsync();

				using (var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection))
				{
					while (await rdr.ReadAsync())
					{
						result.Add(new Fortune
						{
							Id = rdr.GetInt32(0),
							Message = rdr.GetString(1)
						});
					}
				}
			}

			result.Add(new Fortune { Message = "Additional fortune added at request time." });
			result.Sort();

			return result;
		}

		string _connectionString = "server=127.0.0.1;user id=mysqltest;password='test;key=\"val';database=hello_world;ssl mode=none;Default Command Timeout=0;Connection Timeout=0;Connection Reset=false";
	}

	public class OpenConnection
	{
		[Benchmark]
		public void OpenSync()
		{
			using (var connection = new MySqlConnection(s_connectionString))
			{
				connection.Open();
			}
		}

		[Benchmark]
		public async Task OpenAsync()
		{
			using (var connection = new MySqlConnection(s_connectionString))
			{
				await connection.OpenAsync();
			}
		}

		static string s_connectionString = "server=127.0.0.1;user id=mysqltest;password='test;key=\"val';ssl mode=none;Default Command Timeout=0;Connect Timeout=0";
	}

	public class MySqlClient
	{
		[GlobalSetup]
		public void GlobalSetup()
		{
			using (var connection = new MySqlConnection(s_connectionString))
			{
				connection.Open();
				using (var cmd = connection.CreateCommand())
				{
					cmd.CommandText = @"
create schema if not exists benchmark;

drop table if exists benchmark.integers;
create table benchmark.integers (value int not null primary key);
insert into benchmark.integers(value) values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

drop table if exists benchmark.blobs;
create table benchmark.blobs(
rowid integer not null primary key auto_increment,
`Blob` longblob null
);
insert into benchmark.blobs(`Blob`) values(null), (@Blob1), (@Blob2);";

					// larger blobs make the tests run much slower
					AddBlobParameter(cmd, "@Blob1", 75000);
					AddBlobParameter(cmd, "@Blob2", 150000);

					cmd.ExecuteNonQuery();
				}
			}

			s_connectionString += ";database=benchmark";

			m_connection = new MySqlConnection(s_connectionString);
			m_connection.Open();
		}

		[GlobalCleanup]
		public void GlobalCleanup()
		{
			m_connection.Dispose();
			m_connection = null;
			MySqlConnection.ClearAllPools();
		}

		private static void AddBlobParameter(DbCommand command, string name, int size)
		{
			var parameter = command.CreateParameter();
			parameter.ParameterName = name;

			var random = new Random(size);
			var value = new byte[size];
			random.NextBytes(value);
			parameter.Value = value;

			command.Parameters.Add(parameter);
		}

		[Benchmark]
		public async Task OpenFromPoolAsync()
		{
			m_connection.Close();
			await m_connection.OpenAsync();
		}

		[Benchmark]
		public void OpenFromPoolSync()
		{
			m_connection.Close();
			m_connection.Open();
		}

		[Benchmark]
		public async Task ExecuteScalarAsync()
		{
			using (var cmd = m_connection.CreateCommand())
			{
				cmd.CommandText = c_executeScalarSql;
				await cmd.ExecuteScalarAsync();
			}
		}

		[Benchmark]
		public void ExecuteScalarSync()
		{
			using (var cmd = m_connection.CreateCommand())
			{
				cmd.CommandText = c_executeScalarSql;
				cmd.ExecuteScalar();
			}
		}

		private const string c_executeScalarSql = "select max(value) from integers;";

		[Benchmark] public Task ReadBlobsAsync() => ReadAllRowsAsync(c_readBlobsSql);
		[Benchmark] public void ReadBlobsSync() => ReadAllRowsSync(c_readBlobsSql);

		private const string c_readBlobsSql = "select `Blob` from blobs;";

		[Benchmark] public Task ManyRowsAsync() => ReadAllRowsAsync(c_manyRowsSql);
		[Benchmark] public void ManyRowsSync() => ReadAllRowsSync(c_manyRowsSql);

		private const string c_manyRowsSql = "select * from integers a join integers b join integers c;";

		private async Task<int> ReadAllRowsAsync(string sql)
		{
			int total = 0;
			using (var cmd = m_connection.CreateCommand())
			{
				cmd.CommandText = sql;
				using (var reader = await cmd.ExecuteReaderAsync())
				{
					do
					{
						while (await reader.ReadAsync())
						{
							if (reader.FieldCount > 1)
								total += reader.GetInt32(1);
						}
					} while (await reader.NextResultAsync());
				}
			}
			return total;
		}

		private int ReadAllRowsSync(string sql)
		{
			int total = 0;
			using (var cmd = m_connection.CreateCommand())
			{
				cmd.CommandText = sql;
				using (var reader = cmd.ExecuteReader())
				{
					do
					{
						while (reader.Read())
						{
							if (reader.FieldCount > 1)
								total += reader.GetInt32(1);
						}
					} while (reader.NextResult());
				}
			}
			return total;
		}

		// TODO: move to config file
		static string s_connectionString = "server=127.0.0.1;user id=mysqltest;password='test;key=\"val';port=3306;ssl mode=none;Use Affected Rows=true;Connection Reset=false;Default Command Timeout=0";

		MySqlConnection m_connection;
	}
}
