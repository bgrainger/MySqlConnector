using System;
using System.IO;
using System.Linq;
using MySql.Data.MySqlClient;
using Xunit;
using Dapper;
using Xunit.Sdk;

namespace SideBySide
{
	[Collection("BulkLoaderCollection")]
	public class BulkLoaderSync : IClassFixture<DatabaseFixture>
	{
		public BulkLoaderSync(DatabaseFixture database)
		{
			m_database = database;

			m_testTable = "BulkLoaderSyncTest";
			var initializeTable = $@"
				drop table if exists {m_testTable};
				create table {m_testTable}
				(
					one int primary key
					, ignore_one int
					, two varchar(200)
					, ignore_two varchar(200)
					, three varchar(200)
					, four datetime
					, five blob
				) CHARACTER SET = UTF8;";
			m_database.Connection.Execute(initializeTable);

			m_memoryStreamBytes = System.Text.Encoding.UTF8.GetBytes(@"1,'two-1','three-1'
2,'two-2','three-2'
3,'two-3','three-3'
4,'two-4','three-4'
5,'two-5','three-5'
");
		}

#if !BASELINE
		[Fact]
		public void FileNameAndSourceStream()
		{
			var bl = new MySqlBulkLoader(m_database.Connection)
			{
				FileName = "test.dat",
				SourceStream = new MemoryStream(),
				TableName = m_testTable,
			};
			Assert.Throws<InvalidOperationException>(() => bl.Load());
		}
#endif

		[SkippableFact(ConfigSettings.TsvFile)]
		public void BulkLoadTsvFile()
		{
			MySqlBulkLoader bl = new MySqlBulkLoader(m_database.Connection);
			bl.FileName = AppConfig.MySqlBulkLoaderTsvFile;
			bl.TableName = m_testTable;
			bl.Columns.AddRange(new string[] { "one", "two", "three", "four", "five" });
			bl.NumberOfLinesToSkip = 1;
			bl.Expressions.Add("five = UNHEX(five)");
			bl.Local = false;
			int rowCount = bl.Load();
			Assert.Equal(20, rowCount);
		}

		[SkippableFact(ConfigSettings.LocalTsvFile)]
		public void BulkLoadLocalTsvFile()
		{
			MySqlBulkLoader bl = new MySqlBulkLoader(m_database.Connection);
			bl.FileName = AppConfig.MySqlBulkLoaderLocalTsvFile;
			bl.TableName = m_testTable;
			bl.Columns.AddRange(new string[] { "one", "two", "three", "four", "five" });
			bl.NumberOfLinesToSkip = 1;
			bl.Expressions.Add("five = UNHEX(five)");
			bl.Local = true;
			int rowCount = bl.Load();
			Assert.Equal(20, rowCount);
		}

		[SkippableFact(ConfigSettings.LocalTsvFile)]
		public void BulkLoadLocalTsvFileDoubleEscapedTerminators()
		{
			MySqlBulkLoader bl = new MySqlBulkLoader(m_database.Connection);
			bl.FileName = AppConfig.MySqlBulkLoaderLocalTsvFile;
			bl.TableName = m_testTable;
			bl.Columns.AddRange(new string[] { "one", "two", "three", "four", "five" });
			bl.NumberOfLinesToSkip = 1;
			bl.Expressions.Add("five = UNHEX(five)");
			bl.LineTerminator = "\\n";
			bl.FieldTerminator = "\\t";
			bl.Local = true;
			int rowCount = bl.Load();
			Assert.Equal(20, rowCount);
		}

		[SkippableFact(ConfigSettings.CsvFile)]
		public void BulkLoadCsvFile()
		{
			MySqlBulkLoader bl = new MySqlBulkLoader(m_database.Connection);
			bl.FileName = AppConfig.MySqlBulkLoaderCsvFile;
			bl.TableName = m_testTable;
			bl.CharacterSet = "UTF8";
			bl.Columns.AddRange(new string[] { "one", "two", "three", "four", "five" });
			bl.NumberOfLinesToSkip = 1;
			bl.FieldTerminator = ",";
			bl.FieldQuotationCharacter = '"';
			bl.FieldQuotationOptional = true;
			bl.Expressions.Add("five = UNHEX(five)");
			bl.Local = false;
			int rowCount = bl.Load();
			Assert.Equal(20, rowCount);
		}

		[SkippableFact(ConfigSettings.LocalCsvFile)]
		public void BulkLoadLocalCsvFile()
		{
			MySqlBulkLoader bl = new MySqlBulkLoader(m_database.Connection);
			bl.FileName = AppConfig.MySqlBulkLoaderLocalCsvFile;
			bl.TableName = m_testTable;
			bl.CharacterSet = "UTF8";
			bl.Columns.AddRange(new string[] { "one", "two", "three", "four", "five" });
			bl.NumberOfLinesToSkip = 1;
			bl.FieldTerminator = ",";
			bl.FieldQuotationCharacter = '"';
			bl.FieldQuotationOptional = true;
			bl.Expressions.Add("five = UNHEX(five)");
			bl.Local = true;
			int rowCount = bl.Load();
			Assert.Equal(20, rowCount);
		}

		[Fact]
		public void BulkLoadCsvFileNotFound()
		{
			var secureFilePath = m_database.Connection.ExecuteScalar<string>(@"select @@global.secure_file_priv;");
			if (string.IsNullOrEmpty(secureFilePath) || secureFilePath == "NULL")
				return;

			MySqlBulkLoader bl = new MySqlBulkLoader(m_database.Connection);
			bl.FileName = Path.Combine(secureFilePath, AppConfig.MySqlBulkLoaderCsvFile + "-junk");
			bl.TableName = m_testTable;
			bl.CharacterSet = "UTF8";
			bl.Columns.AddRange(new string[] { "one", "two", "three", "four", "five" });
			bl.NumberOfLinesToSkip = 1;
			bl.FieldTerminator = ",";
			bl.FieldQuotationCharacter = '"';
			bl.FieldQuotationOptional = true;
			bl.Expressions.Add("five = UNHEX(five)");
			bl.Local = false;
			try
			{
				int rowCount = bl.Load();
			}
			catch (Exception exception)
			{
				while (exception.InnerException != null)
					exception = exception.InnerException;

				if (!(exception is FileNotFoundException))
				{
					try
					{
						Assert.Contains("Errcode: 2 ", exception.Message, StringComparison.OrdinalIgnoreCase);
					}
					catch (ContainsException)
					{
						Assert.Contains("OS errno 2 ", exception.Message, StringComparison.OrdinalIgnoreCase);
					}
					Assert.Contains("No such file or directory", exception.Message);
				}
			}
		}

		[Fact]
		public void BulkLoadLocalCsvFileNotFound()
		{
			MySqlBulkLoader bl = new MySqlBulkLoader(m_database.Connection);
			bl.Timeout = 3; //Set a short timeout for this test because the file not found exception takes a long time otherwise, the timeout does not change the result
			bl.FileName = AppConfig.MySqlBulkLoaderLocalCsvFile + "-junk";
			bl.TableName = m_testTable;
			bl.CharacterSet = "UTF8";
			bl.Columns.AddRange(new string[] { "one", "two", "three", "four", "five" });
			bl.NumberOfLinesToSkip = 1;
			bl.FieldTerminator = ",";
			bl.FieldQuotationCharacter = '"';
			bl.FieldQuotationOptional = true;
			bl.Expressions.Add("five = UNHEX(five)");
			bl.Local = true;
			try
			{
				int rowCount = bl.Load();
			}
			catch (MySqlException mySqlException)
			{
				while (mySqlException.InnerException != null)
				{
					if (mySqlException.InnerException.GetType() == typeof(MySqlException))
					{
						mySqlException = (MySqlException)mySqlException.InnerException;
					}
					else
					{
						Assert.IsType<System.IO.FileNotFoundException>(mySqlException.InnerException);
						break;
					}
				}
				if (mySqlException.InnerException == null)
				{
					Assert.IsType<System.IO.FileNotFoundException>(mySqlException);
				}
			}
			catch (Exception exception)
			{
				//We know that the exception is not a MySqlException, just use the assertion to fail the test
				Assert.IsType<MySqlException>(exception);
			};
		}

		[SkippableFact(ConfigSettings.LocalCsvFile)]
		public void BulkLoadLocalCsvFileInTransactionWithCommit()
		{
			try
			{
				m_database.Connection.Open();
				using (var transaction = m_database.Connection.BeginTransaction())
				{
					var bulkLoader = new MySqlBulkLoader(m_database.Connection)
					{
						FileName = AppConfig.MySqlBulkLoaderLocalCsvFile,
						TableName = m_testTable,
						CharacterSet = "UTF8",
						NumberOfLinesToSkip = 1,
						FieldTerminator = ",",
						FieldQuotationCharacter = '"',
						FieldQuotationOptional = true,
						Local = true,
					};
					bulkLoader.Expressions.Add("five = UNHEX(five)");
					bulkLoader.Columns.AddRange(new[] { "one", "two", "three", "four", "five" });

					var rowCount = bulkLoader.Load();
					Assert.Equal(20, rowCount);

					transaction.Commit();
				}

				Assert.Equal(20, m_database.Connection.ExecuteScalar<int>($@"select count(*) from {m_testTable};"));
			}
			finally
			{
				m_database.Connection.Close();
			}
		}

		[SkippableFact(ConfigSettings.LocalCsvFile)]
		public void BulkLoadLocalCsvFileBeforeTransactionWithCommit()
		{
			try
			{
				m_database.Connection.Open();
				var bulkLoader = new MySqlBulkLoader(m_database.Connection)
				{
					FileName = AppConfig.MySqlBulkLoaderLocalCsvFile,
					TableName = m_testTable,
					CharacterSet = "UTF8",
					NumberOfLinesToSkip = 1,
					FieldTerminator = ",",
					FieldQuotationCharacter = '"',
					FieldQuotationOptional = true,
					Local = true,
				};
				bulkLoader.Expressions.Add("five = UNHEX(five)");
				bulkLoader.Columns.AddRange(new[] { "one", "two", "three", "four", "five" });

				using (var transaction = m_database.Connection.BeginTransaction())
				{
					var rowCount = bulkLoader.Load();
					Assert.Equal(20, rowCount);

					transaction.Commit();
				}

				Assert.Equal(20, m_database.Connection.ExecuteScalar<int>($@"select count(*) from {m_testTable};"));
			}
			finally
			{
				m_database.Connection.Close();
			}
		}

		[SkippableFact(ConfigSettings.LocalCsvFile)]
		public void BulkLoadLocalCsvFileInTransactionWithRollback()
		{
			try
			{
				m_database.Connection.Open();
				using (var transaction = m_database.Connection.BeginTransaction())
				{
					var bulkLoader = new MySqlBulkLoader(m_database.Connection)
					{
						FileName = AppConfig.MySqlBulkLoaderLocalCsvFile,
						TableName = m_testTable,
						CharacterSet = "UTF8",
						NumberOfLinesToSkip = 1,
						FieldTerminator = ",",
						FieldQuotationCharacter = '"',
						FieldQuotationOptional = true,
						Local = true,
					};
					bulkLoader.Expressions.Add("five = UNHEX(five)");
					bulkLoader.Columns.AddRange(new[] { "one", "two", "three", "four", "five" });

					var rowCount = bulkLoader.Load();
					Assert.Equal(20, rowCount);

					transaction.Rollback();
				}

				Assert.Equal(0, m_database.Connection.ExecuteScalar<int>($@"select count(*) from {m_testTable};"));
			}
			finally
			{
				m_database.Connection.Close();
			}
		}

		[SkippableFact(ConfigSettings.LocalCsvFile)]
		public void BulkLoadLocalCsvFileBeforeTransactionWithRollback()
		{
			try
			{
				m_database.Connection.Open();
				var bulkLoader = new MySqlBulkLoader(m_database.Connection)
				{
					FileName = AppConfig.MySqlBulkLoaderLocalCsvFile,
					TableName = m_testTable,
					CharacterSet = "UTF8",
					NumberOfLinesToSkip = 1,
					FieldTerminator = ",",
					FieldQuotationCharacter = '"',
					FieldQuotationOptional = true,
					Local = true,
				};
				bulkLoader.Expressions.Add("five = UNHEX(five)");
				bulkLoader.Columns.AddRange(new[] { "one", "two", "three", "four", "five" });

				using (var transaction = m_database.Connection.BeginTransaction())
				{
					var rowCount = bulkLoader.Load();
					Assert.Equal(20, rowCount);

					transaction.Rollback();
				}

				Assert.Equal(0, m_database.Connection.ExecuteScalar<int>($@"select count(*) from {m_testTable};"));
			}
			finally
			{
				m_database.Connection.Close();
			}
		}

		[Fact]
		public void BulkLoadMissingFileName()
		{
			MySqlBulkLoader bl = new MySqlBulkLoader(m_database.Connection);
			bl.TableName = m_testTable;
			bl.CharacterSet = "UTF8";
			bl.Columns.AddRange(new string[] { "one", "two", "three", "four", "five" });
			bl.NumberOfLinesToSkip = 1;
			bl.FieldTerminator = ",";
			bl.FieldQuotationCharacter = '"';
			bl.FieldQuotationOptional = true;
			bl.Expressions.Add("five = UNHEX(five)");
			bl.Local = false;
#if BASELINE
			Assert.Throws<System.NullReferenceException>(() =>
			{
				int rowCount = bl.Load();
			});
#else
			Assert.Throws<System.InvalidOperationException>(() =>
			{
				int rowCount = bl.Load();
			});
#endif
		}

		[SkippableFact(ConfigSettings.LocalCsvFile)]
		public void BulkLoadMissingTableName()
		{
			MySqlBulkLoader bl = new MySqlBulkLoader(m_database.Connection);
			bl.FileName = AppConfig.MySqlBulkLoaderLocalCsvFile;
			bl.Columns.AddRange(new string[] { "one", "two", "three", "four", "five" });
			bl.NumberOfLinesToSkip = 1;
			bl.FieldTerminator = ",";
			bl.FieldQuotationCharacter = '"';
			bl.FieldQuotationOptional = true;
			bl.Expressions.Add("five = UNHEX(five)");
			bl.Local = false;
#if BASELINE
			Assert.Throws<MySqlException>(() =>
			{
				int rowCount = bl.Load();
			});
#else
			Assert.Throws<System.InvalidOperationException>(() =>
			{
				int rowCount = bl.Load();
			});
#endif
		}

#if !BASELINE
		[SkippableFact(ConfigSettings.LocalCsvFile)]
		public void BulkLoadFileStreamInvalidOperation()
		{
			var bl = new MySqlBulkLoader(m_database.Connection);
			using var fileStream = new FileStream(AppConfig.MySqlBulkLoaderLocalCsvFile, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);
			bl.SourceStream = fileStream;
			bl.TableName = m_testTable;
			bl.CharacterSet = "UTF8";
			bl.Columns.AddRange(new string[] { "one", "two", "three", "four", "five" });
			bl.NumberOfLinesToSkip = 1;
			bl.FieldTerminator = ",";
			bl.FieldQuotationCharacter = '"';
			bl.FieldQuotationOptional = true;
			bl.Expressions.Add("five = UNHEX(five)");
			bl.Local = false;
			Assert.Throws<System.InvalidOperationException>(() =>
			{
				int rowCount = bl.Load();
			});
		}

		[SkippableFact(ConfigSettings.LocalCsvFile)]
		public void BulkLoadLocalFileStream()
		{
			var bl = new MySqlBulkLoader(m_database.Connection);
			using var fileStream = new FileStream(AppConfig.MySqlBulkLoaderLocalCsvFile, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);
			bl.SourceStream = fileStream;
			bl.TableName = m_testTable;
			bl.CharacterSet = "UTF8";
			bl.Columns.AddRange(new string[] { "one", "two", "three", "four", "five" });
			bl.NumberOfLinesToSkip = 1;
			bl.FieldTerminator = ",";
			bl.FieldQuotationCharacter = '"';
			bl.FieldQuotationOptional = true;
			bl.Expressions.Add("five = UNHEX(five)");
			bl.Local = true;
			int rowCount = bl.Load();
			Assert.Equal(20, rowCount);
		}

		[Fact]
		public void BulkLoadMemoryStreamInvalidOperation()
		{
			var bl = new MySqlBulkLoader(m_database.Connection);
			using var memoryStream = new MemoryStream(m_memoryStreamBytes, false);
			bl.SourceStream = memoryStream;
			bl.TableName = m_testTable;
			bl.CharacterSet = "UTF8";
			bl.Columns.AddRange(new string[] { "one", "two", "three" });
			bl.NumberOfLinesToSkip = 0;
			bl.FieldTerminator = ",";
			bl.FieldQuotationCharacter = '"';
			bl.FieldQuotationOptional = true;
			bl.Local = false;
			Assert.Throws<System.InvalidOperationException>(() =>
			{
				int rowCount = bl.Load();
			});
		}

		[Fact]
		public void BulkLoadLocalMemoryStream()
		{
			var bl = new MySqlBulkLoader(m_database.Connection);
			using var memoryStream = new MemoryStream(m_memoryStreamBytes, false);
			bl.SourceStream = memoryStream;
			bl.TableName = m_testTable;
			bl.CharacterSet = "UTF8";
			bl.Columns.AddRange(new string[] { "one", "two", "three" });
			bl.NumberOfLinesToSkip = 0;
			bl.FieldTerminator = ",";
			bl.FieldQuotationCharacter = '"';
			bl.FieldQuotationOptional = true;
			bl.Local = true;
			int rowCount = bl.Load();
			Assert.Equal(5, rowCount);
		}
#endif

		readonly DatabaseFixture m_database;
		readonly string m_testTable;
		readonly byte[] m_memoryStreamBytes;
	}
}
