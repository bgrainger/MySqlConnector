using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.ExceptionServices;
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Payloads;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Core;

internal sealed class ResultSet(MySqlDataReader dataReader)
{
	public void Reset()
	{
		// ResultSet can be re-used, so initialize everything
		BufferState = ResultSetState.None;
		m_columnDefinitions = default;
		WarningCount = 0;
		State = ResultSetState.None;
		ContainsCommandParameters = false;
		m_columnDefinitionPayloadUsedBytes = 0;
		m_readBuffer?.Clear();
		m_row = null;
		m_hasRows = false;
		ReadResultSetHeaderException = null;
	}

	public async Task ReadResultSetHeaderAsync(IOBehavior ioBehavior)
	{
		Reset();

		try
		{
			while (true)
			{
				var payload = await Session.ReceiveReplyAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
				var firstByte = payload.HeaderByte;
				if (firstByte == OkPayload.Signature)
				{
					var ok = OkPayload.Create(payload.Span, Session);

					// if we've read a result set header then this is a SELECT statement, so we shouldn't overwrite RecordsAffected
					// (which should be -1 for SELECT) unless the server reports a non-zero count
					if (State != ResultSetState.ReadResultSetHeader || ok.AffectedRowCount != 0)
						DataReader.RealRecordsAffected = (DataReader.RealRecordsAffected ?? 0) + ok.AffectedRowCount;

					if (ok.LastInsertId != 0)
						Command?.SetLastInsertedId((long) ok.LastInsertId);
					WarningCount = ok.WarningCount;
					if (ok.NewSchema is not null)
						Connection.Session.DatabaseOverride = ok.NewSchema;
					m_columnDefinitions = default;
					State = (ok.ServerStatus & ServerStatus.MoreResultsExist) == 0
						? ResultSetState.NoMoreData
						: ResultSetState.HasMoreData;
					if (State == ResultSetState.NoMoreData)
						break;
				}
				else if (firstByte == LocalInfilePayload.Signature)
				{
					try
					{
						if (!Connection.AllowLoadLocalInfile)
							throw new NotSupportedException("To use LOAD DATA LOCAL INFILE, set AllowLoadLocalInfile=true in the connection string. See https://mysqlconnector.net/load-data");
						var localInfile = LocalInfilePayload.Create(payload.Span);
						var hasSourcePrefix = localInfile.FileName.StartsWith(MySqlBulkLoader.SourcePrefix, StringComparison.Ordinal);
						if (!IsHostVerified(Connection) && !hasSourcePrefix)
							throw new NotSupportedException("Use SourceStream or SslMode >= VerifyCA for LOAD DATA LOCAL INFILE. See https://mysqlconnector.net/load-data");

						var source = hasSourcePrefix ?
							MySqlBulkLoader.GetAndRemoveSource(localInfile.FileName) :
							File.OpenRead(localInfile.FileName);
						switch (source)
						{
							case Stream stream:
								var buffer = ArrayPool<byte>.Shared.Rent(1048576);
								try
								{
									int byteCount;
									while ((byteCount = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false)) > 0)
									{
										payload = new(new ArraySegment<byte>(buffer, 0, byteCount));
										await Session.SendReplyAsync(payload, ioBehavior, CancellationToken.None).ConfigureAwait(false);
									}
								}
								finally
								{
									ArrayPool<byte>.Shared.Return(buffer);
									stream.Dispose();
								}
								break;

							case MySqlBulkCopy bulkCopy:
								await bulkCopy.SendDataReaderAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
								break;

							default:
								throw new InvalidOperationException($"Unsupported Source type: {source.GetType().Name}");
						}
					}
					catch (Exception ex)
					{
						// store the exception, to be thrown after reading the response packet from the server
						ReadResultSetHeaderException = ExceptionDispatchInfo.Capture(new MySqlException("Error during LOAD DATA LOCAL INFILE", ex));
					}

					await Session.SendReplyAsync(EmptyPayload.Instance, ioBehavior, CancellationToken.None).ConfigureAwait(false);
				}
				else
				{
					var columnCountPacket = ColumnCountPayload.Create(payload.Span, Session.SupportsCachedPreparedMetadata);
					var columnCount = columnCountPacket.ColumnCount;
					if (!columnCountPacket.MetadataFollows)
					{
						// reuse previous metadata
						m_columnDefinitions = DataReader.LastUsedPreparedStatement!.Columns!;
						if (m_columnDefinitions.Length != columnCount)
							throw new InvalidOperationException($"Expected result set to have {m_columnDefinitions.Length} columns, but it contains {columnCount} columns");
					}
					else
					{
						// parse columns
						// reserve adequate space to hold a copy of all column definitions (but note that this can be resized below if we guess too small)
						Utility.Resize(ref m_columnDefinitionPayloadBytes, columnCount * 96);

						// increase the cache size to be large enough to hold all the column definitions
						if (m_columnDefinitionPayloadCache is null)
							m_columnDefinitionPayloadCache = new ColumnDefinitionPayload[columnCount];
						else if (m_columnDefinitionPayloadCache.Length < columnCount)
							Array.Resize(ref m_columnDefinitionPayloadCache, Math.Max(columnCount, m_columnDefinitionPayloadCache.Length * 2));
						m_columnDefinitions = m_columnDefinitionPayloadCache.AsMemory(0, columnCount);

						// if the server supports metadata caching but has re-sent it, something has changed since last prepare/execution and we need to update the columns
						var preparedColumns = Session.SupportsCachedPreparedMetadata ? DataReader.LastUsedPreparedStatement?.Columns : null;

						for (var column = 0; column < columnCount; column++)
						{
							payload = await Session.ReceiveReplyAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
							var payloadLength = payload.Span.Length;

							// 'Session.ReceiveReplyAsync' reuses a shared buffer; make a copy so that the column definitions can always be safely read at any future point
							if (m_columnDefinitionPayloadUsedBytes + payloadLength > m_columnDefinitionPayloadBytes.Count)
								Utility.Resize(ref m_columnDefinitionPayloadBytes, m_columnDefinitionPayloadUsedBytes + payloadLength);
							payload.Span.CopyTo(m_columnDefinitionPayloadBytes.AsSpan(m_columnDefinitionPayloadUsedBytes));

							// create/update the column definition in our cache
							var payloadBytesSegment = new ResizableArraySegment<byte>(m_columnDefinitionPayloadBytes, m_columnDefinitionPayloadUsedBytes, payloadLength);
							ColumnDefinitionPayload.Initialize(ref m_columnDefinitionPayloadCache[column], payloadBytesSegment);

							// if there was a prepared statement, update its cached columns too
							if (preparedColumns is not null)
								ColumnDefinitionPayload.Initialize(ref preparedColumns[column], payloadBytesSegment);

							m_columnDefinitionPayloadUsedBytes += payloadLength;
						}
					}

					if (!Session.SupportsDeprecateEof)
					{
						payload = await Session.ReceiveReplyAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
						_ = EofPayload.Create(payload.Span);
					}

					if (ColumnDefinitions.Length == (Command?.OutParameters?.Count + 1) && ColumnDefinitions[0].Name == SingleCommandPayloadCreator.OutParameterSentinelColumnName)
						ContainsCommandParameters = true;
					WarningCount = 0;
					State = ResultSetState.ReadResultSetHeader;
					if (DataReader.Activity is { IsAllDataRequested: true } && (Command?.Connection!.MySqlDataSource?.TracingOptions.EnableResultSetHeaderEvent ?? MySqlConnectorTracingOptions.Default.EnableResultSetHeaderEvent))
						DataReader.Activity.AddEvent(new ActivityEvent("read-result-set-header"));
					break;
				}
			}
		}
		catch (Exception ex)
		{
			ReadResultSetHeaderException = ExceptionDispatchInfo.Capture(ex);
		}
		finally
		{
			BufferState = State;
		}
	}

	private static bool IsHostVerified(MySqlConnection connection) =>
		connection.SslMode is MySqlSslMode.VerifyCA or MySqlSslMode.VerifyFull;

	public async Task ReadEntireAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		while (State is ResultSetState.ReadingRows or ResultSetState.ReadResultSetHeader)
			_ = await ReadAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
	}

	public bool Read()
	{
		return ReadAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();
	}

	public Task<bool> ReadAsync(CancellationToken cancellationToken) =>
		ReadAsync(Connection.AsyncIOBehavior, cancellationToken);

	public async Task<bool> ReadAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		m_row = m_readBuffer?.Count > 0 ? m_readBuffer.Dequeue() :
			await ScanRowAsync(ioBehavior, m_row, cancellationToken).ConfigureAwait(false);

		if (Command.ReturnParameter is not null && m_row is not null)
		{
			Command.ReturnParameter.Value = m_row.GetValue(0);
			Command.ReturnParameter = null;
		}

		if (m_row is null)
		{
			State = BufferState;
			return false;
		}
		State = ResultSetState.ReadingRows;
		return true;
	}

	public async Task<Row?> BufferReadAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
	{
		var row = await ScanRowAsync(ioBehavior, null, cancellationToken).ConfigureAwait(false);
		if (row is null)
			return null;
		m_readBuffer ??= new();
		m_readBuffer.Enqueue(row);
		return row;
	}

	private async ValueTask<Row?> ScanRowAsync(IOBehavior ioBehavior, Row? row, CancellationToken cancellationToken)
	{
		// if we've already read past the end of this resultset, Read returns false
		if (BufferState is ResultSetState.HasMoreData or ResultSetState.NoMoreData or ResultSetState.None)
			return null;

		PayloadData payload;
		try
		{
			payload = await Session.ReceiveReplyAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
		}
		catch (MySqlException ex)
		{
			BufferState = State = ResultSetState.NoMoreData;
			if (ex.ErrorCode == MySqlErrorCode.QueryInterrupted && cancellationToken.IsCancellationRequested)
				throw new OperationCanceledException(ex.Message, ex, cancellationToken);
			if (ex.ErrorCode == MySqlErrorCode.QueryInterrupted && Command.CancellableCommand.IsTimedOut)
				throw MySqlException.CreateForTimeout(ex);
			throw;
		}

		if (payload.HeaderByte == EofPayload.Signature)
		{
			if (Session.SupportsDeprecateEof && OkPayload.IsOk(payload.Span, Session))
			{
				var ok = OkPayload.Create(payload.Span, Session);
				BufferState = (ok.ServerStatus & ServerStatus.MoreResultsExist) == 0 ? ResultSetState.NoMoreData : ResultSetState.HasMoreData;
				return null;
			}
			if (!Session.SupportsDeprecateEof && EofPayload.IsEof(payload))
			{
				var eof = EofPayload.Create(payload.Span);
				BufferState = (eof.ServerStatus & ServerStatus.MoreResultsExist) == 0 ? ResultSetState.NoMoreData : ResultSetState.HasMoreData;
				return null;
			}
		}

		row ??= new Row(Command.TryGetPreparedStatements() is not null, this);
		row.SetData(payload.Memory);
		m_hasRows = true;
		BufferState = ResultSetState.ReadingRows;
		return row;
	}

#pragma warning disable CA1822 // Mark members as static
	public int Depth => 0;
#pragma warning restore CA1822 // Mark members as static

#pragma warning disable CA2201 // Do not raise reserved exception types (IndexOutOfRangeException)

	public string GetName(int ordinal)
	{
		if (!HasResultSet)
			throw new InvalidOperationException("There is no current result set.");
		if (ordinal < 0 || ordinal >= ColumnDefinitions.Length)
			throw new IndexOutOfRangeException($"value must be between 0 and {ColumnDefinitions.Length - 1}");
		return ColumnDefinitions[ordinal].Name;
	}

	public string GetDataTypeName(int ordinal)
	{
		if (!HasResultSet)
			throw new InvalidOperationException("There is no current result set.");
		if (ordinal < 0 || ordinal >= ColumnDefinitions.Length)
			throw new IndexOutOfRangeException($"value must be between 0 and {ColumnDefinitions.Length - 1}");

		var mySqlDbType = GetColumnType(ordinal);
		if (mySqlDbType == MySqlDbType.String)
			return string.Format(CultureInfo.InvariantCulture, "CHAR({0})", ColumnDefinitions[ordinal].ColumnLength / ColumnDefinitions[ordinal].CharacterSet.BytesPerCharacter);
		return TypeMapper.Instance.GetColumnTypeMetadata(mySqlDbType).SimpleDataTypeName;
	}

	public Type GetFieldType(int ordinal)
	{
		if (!HasResultSet)
			throw new InvalidOperationException("There is no current result set.");
		if (ordinal < 0 || ordinal >= ColumnDefinitions.Length)
			throw new IndexOutOfRangeException($"value must be between 0 and {ColumnDefinitions.Length - 1}");

		var type = TypeMapper.Instance.GetColumnTypeMetadata(GetColumnType(ordinal)).DbTypeMapping.ClrType;
		if (Connection.AllowZeroDateTime && type == typeof(DateTime))
			type = typeof(MySqlDateTime);
		return type;
	}

	public MySqlDbType GetColumnType(int ordinal) =>
		TypeMapper.ConvertToMySqlDbType(ColumnDefinitions[ordinal], Connection.TreatTinyAsBoolean, Connection.GuidFormat);

	public int FieldCount => ColumnDefinitions.Length;

	public bool HasRows
	{
		get
		{
			if (BufferState == ResultSetState.ReadResultSetHeader)
				return BufferReadAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult() is not null;
			return m_hasRows;
		}
	}

	public int GetOrdinal(string name)
	{
#if NET6_0_OR_GREATER
		ArgumentNullException.ThrowIfNull(name);
#else
		if (name is null)
			throw new ArgumentNullException(nameof(name));
#endif
		if (!HasResultSet)
			throw new InvalidOperationException("There is no current result set.");

		for (var column = 0; column < ColumnDefinitions.Length; column++)
		{
			if (name.Equals(ColumnDefinitions[column].Name, StringComparison.OrdinalIgnoreCase))
				return column;
		}

		throw new IndexOutOfRangeException($"The column name '{name}' does not exist in the result set.");
	}

	public Row GetCurrentRow()
	{
		if (State != ResultSetState.ReadingRows)
			throw new InvalidOperationException("Read must be called first.");
		return m_row ?? throw new InvalidOperationException("There is no current row.");
	}

	public MySqlDataReader DataReader { get; } = dataReader;
	public ExceptionDispatchInfo? ReadResultSetHeaderException { get; private set; }
	public IMySqlCommand Command => DataReader.Command!;
	public MySqlConnection Connection => DataReader.Connection!;
	public ServerSession Session => DataReader.Session!;

	public ResultSetState BufferState { get; private set; }
	public ReadOnlySpan<ColumnDefinitionPayload> ColumnDefinitions => m_columnDefinitions.Span;
	public int WarningCount { get; private set; }
	public ResultSetState State { get; private set; }
	public bool HasResultSet => !(State == ResultSetState.None || ColumnDefinitions.Length == 0);
	public bool ContainsCommandParameters { get; private set; }

	private ResizableArray<byte>? m_columnDefinitionPayloadBytes;
	private int m_columnDefinitionPayloadUsedBytes;
	private Queue<Row>? m_readBuffer;
	private Row? m_row;
	private bool m_hasRows;
	private ReadOnlyMemory<ColumnDefinitionPayload> m_columnDefinitions;
	private ColumnDefinitionPayload[]? m_columnDefinitionPayloadCache;
}
