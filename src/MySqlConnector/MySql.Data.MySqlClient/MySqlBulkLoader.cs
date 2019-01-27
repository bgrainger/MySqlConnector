using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector.Protocol.Serialization;

namespace MySql.Data.MySqlClient
{
    public sealed class MySqlBulkLoader
    {
        private const string defaultFieldTerminator = "\t";
        private const string defaultLineTerminator = "\n";
        private const char defaultEscapeCharacter = '\\';

        private static readonly object s_lock = new object();
        private static readonly Dictionary<string, Stream> s_streams = new Dictionary<string, Stream>();

        public string? CharacterSet { get; set; }
        public List<string> Columns { get; }
        public MySqlBulkLoaderConflictOption ConflictOption { get; set; }
        public MySqlConnection Connection { get; set; }
        public char EscapeCharacter { get; set; }
        public List<string> Expressions { get; }
        public char FieldQuotationCharacter { get; set; }
        public bool FieldQuotationOptional { get; set; }
        public string FieldTerminator { get; set; }

        /// <summary>
        /// The name of the local (if <see cref="Local"/> is <c>true</c>) or remote (otherwise) file to load.
        /// Either this or <see cref="SourceStream"/> must be set.
        /// </summary>
        public string? FileName { get; set; }

        public string? LinePrefix { get; set; }
        public string LineTerminator { get; set; }
        public bool Local { get; set; }
        public int NumberOfLinesToSkip { get; set; }
        public MySqlBulkLoaderPriority Priority { get; set; }

        /// <summary>
        /// A <see cref="Stream"/> containing the data to load. Either this or <see cref="FileName"/> must be set.
        /// The <see cref="Local"/> property must be <c>true</c> if this is set.
        /// </summary>
        public Stream? SourceStream { get; set; }

        public string? TableName { get; set; }
        public int Timeout { get; set; }

        public MySqlBulkLoader(MySqlConnection connection)
        {
            Connection = connection;
            Local = true;
            FieldTerminator = defaultFieldTerminator;
            LineTerminator = defaultLineTerminator;
            FieldQuotationCharacter = '\0';
            ConflictOption = MySqlBulkLoaderConflictOption.None;
            Columns = new List<string>();
            Expressions = new List<string>();
        }

        private string BuildSqlCommand()
        {
            StringBuilder sqlCommandMain = new StringBuilder("LOAD DATA ");
            if (Priority == MySqlBulkLoaderPriority.Low)
                sqlCommandMain.Append("LOW_PRIORITY ");
            else if (Priority == MySqlBulkLoaderPriority.Concurrent)
                sqlCommandMain.Append("CONCURRENT ");

            if (Local)
                sqlCommandMain.Append("LOCAL ");

            sqlCommandMain.Append("INFILE ");

            if (System.IO.Path.DirectorySeparatorChar != '\\')
                sqlCommandMain.AppendFormat("'{0}' ", FileName);
            else
                sqlCommandMain.AppendFormat("'{0}' ", FileName!.Replace("\\", "\\\\"));

            if (ConflictOption == MySqlBulkLoaderConflictOption.Ignore)
                sqlCommandMain.Append("IGNORE ");
            else if (ConflictOption == MySqlBulkLoaderConflictOption.Replace)
                sqlCommandMain.Append("REPLACE ");

            sqlCommandMain.AppendFormat("INTO TABLE {0} ", TableName);

            if (CharacterSet != null)
                sqlCommandMain.AppendFormat("CHARACTER SET {0} ", CharacterSet);

            StringBuilder sqlCommandFragment = new StringBuilder();
            if (FieldTerminator != defaultFieldTerminator)
                sqlCommandFragment.AppendFormat("TERMINATED BY \'{0}\' ", FieldTerminator);

            if (FieldQuotationCharacter != 0)
                sqlCommandFragment.AppendFormat("{0} ENCLOSED BY \'{1}\' ", (FieldQuotationOptional ? "OPTIONALLY" : ""), FieldQuotationCharacter);

            if (EscapeCharacter != defaultEscapeCharacter && EscapeCharacter != 0)
                sqlCommandFragment.AppendFormat("ESCAPED BY \'{0}\' ", EscapeCharacter);

            if (sqlCommandFragment.Length > 0)
            {
                sqlCommandMain.AppendFormat("FIELDS {0}", sqlCommandFragment.ToString());
                sqlCommandFragment.Clear();
            }

            if (LinePrefix != null && LinePrefix.Length > 0)
                sqlCommandFragment.AppendFormat("STARTING BY \'{0}\' ", LinePrefix);

            if (LineTerminator != defaultLineTerminator)
                sqlCommandFragment.AppendFormat("TERMINATED BY \'{0}\' ", LineTerminator);

            if (sqlCommandFragment.Length > 0)
                sqlCommandMain.AppendFormat("LINES {0}", sqlCommandFragment.ToString());

            if (NumberOfLinesToSkip > 0)
                sqlCommandMain.AppendFormat("IGNORE {0} LINES ", NumberOfLinesToSkip);

            if (Columns.Count > 0)
            {
                sqlCommandMain.Append("(");
                sqlCommandMain.Append(Columns[0]);
                for (int i = 1; i < Columns.Count; i++)
                    sqlCommandMain.AppendFormat(",{0}", Columns[i]);
                sqlCommandMain.Append(") ");
            }

            if (Expressions.Count > 0)
            {
                sqlCommandMain.Append("SET ");
                sqlCommandMain.Append(Expressions[0]);
                for (int i = 1; i < Expressions.Count; i++)
                    sqlCommandMain.AppendFormat(",{0}", Expressions[i]);
            }

            return sqlCommandMain.ToString();
        }

        public int Load() => LoadAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

        public Task<int> LoadAsync() => LoadAsync(IOBehavior.Asynchronous, CancellationToken.None);

        public Task<int> LoadAsync(CancellationToken cancellationToken) => LoadAsync(IOBehavior.Asynchronous, cancellationToken);

        private async Task<int> LoadAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
        {
            if (Connection == null)
                throw new InvalidOperationException("Connection not set");

            if (!string.IsNullOrWhiteSpace(FileName) && SourceStream != null)
                throw new InvalidOperationException("Cannot set both FileName and SourceStream");

			// LOCAL INFILE case
			if (!string.IsNullOrWhiteSpace(FileName) && Local)
			{
				SourceStream = CreateFileStream(FileName);
				FileName = null;
			}

			if (string.IsNullOrWhiteSpace(FileName) && SourceStream != null)
			{
				if (!Local)
					throw new InvalidOperationException("Cannot use SourceStream when Local is not true.");

				FileName = GenerateSourceStreamName();
				lock (s_lock)
					s_streams.Add(FileName, SourceStream);
			}

            if (string.IsNullOrWhiteSpace(FileName) || string.IsNullOrWhiteSpace(TableName))
            {
                // This is intentionally a different exception to what is thrown by MySql.Data because
                // it does not handle null or empty FileName and TableName.
                // The baseline client simply tries to use the given values, resulting in a NullReferenceException for
                // a null FileName, a MySqlException with an inner FileStream exception for an empty FileName,
                // and a MySqlException with a syntax error if the TableName is null or empty.
                throw new InvalidOperationException("FileName or SourceStream, and TableName are required.");
            }

            bool closeConnection = false;
            if (Connection.State != ConnectionState.Open)
            {
                closeConnection = true;
                Connection.Open();
            }

            try
            {
                var commandString = BuildSqlCommand();
                var cmd = new MySqlCommand(commandString, Connection, Connection.CurrentTransaction)
                {
                    CommandTimeout = Timeout,
                };
                return await cmd.ExecuteNonQueryAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (closeConnection)
                    Connection.Close();
            }
        }

		private Stream CreateFileStream(string fileName)
		{
			try
			{
				return File.OpenRead(fileName);
			}
			catch (Exception ex)
			{
				throw new MySqlException($"Could not access file \"{fileName}\"", ex);
			}
		}

		private static string GenerateSourceStreamName()
		{
			return StreamPrefix + Guid.NewGuid().ToString("N");
		}

		internal const string StreamPrefix = ":STREAM:";

        internal static Stream GetAndRemoveStream(string streamKey)
        {
            lock (s_lock)
            {
                var stream = s_streams[streamKey];
                s_streams.Remove(streamKey);
                return stream;
            }
        }
    }
}
