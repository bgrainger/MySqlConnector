using System.Net.Security;
using System.Security.Authentication;
using Microsoft.Extensions.Logging;
using MySqlConnector.Protocol.Serialization;

namespace MySqlConnector.Logging;

internal static partial class Log
{
	[LoggerMessage(EventIds.DataSourceCreatedWithPoolWithName, LogLevel.Information, "Data source {DataSourceId} created with pool {PoolId} and name {DataSourceName}")]
	public static partial void DataSourceCreatedWithPoolWithName(ILogger logger, int dataSourceId, int poolId, string dataSourceName);

	[LoggerMessage(EventIds.DataSourceCreatedWithoutPoolWithName, LogLevel.Information, "Data source {DataSourceId} created with name {DataSourceName} and no pool")]
	public static partial void DataSourceCreatedWithoutPoolWithName(ILogger logger, int dataSourceId, string dataSourceName);

	[LoggerMessage(EventIds.DataSourceCreatedWithPoolWithoutName, LogLevel.Information, "Data source {DataSourceId} created with pool {PoolId} and no name")]
	public static partial void DataSourceCreatedWithPoolWithoutName(ILogger logger, int dataSourceId, int poolId);

	[LoggerMessage(EventIds.DataSourceCreatedWithoutPoolWithoutName, LogLevel.Information, "Data source {DataSourceId} created with no pool and no name")]
	public static partial void DataSourceCreatedWithoutPoolWithoutName(ILogger logger, int dataSourceId);

	[LoggerMessage(EventIds.PeriodicPasswordProviderFailed, LogLevel.Error, "Periodic password provider for data source {DataSourceId} failed: {ExceptionMessage}")]
	public static partial void PeriodicPasswordProviderFailed(ILogger logger, Exception exception, int dataSourceId, string exceptionMessage);

	[LoggerMessage(EventIds.CreatedNonPooledSession, LogLevel.Debug, "Created new non-pooled session {SessionId}")]
	public static partial void CreatedNonPooledSession(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.ResettingConnection, LogLevel.Debug, "Session {SessionId} resetting connection")]
	public static partial void ResettingConnection(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.ReturningToPool, LogLevel.Trace, "Session {SessionId} returning to pool {PoolId}")]
	public static partial void ReturningToPool(ILogger logger, string sessionId, int poolId);

	[LoggerMessage(EventIds.SendingQuitCommand, LogLevel.Trace, "Session {SessionId} sending QUIT command")]
	public static partial void SendingQuitCommand(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.ConnectingFailed, LogLevel.Error, "Session {SessionId} connecting failed")]
	public static partial void ConnectingFailed(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.ServerSentAuthPluginName, LogLevel.Trace, "Session {SessionId} server sent auth plugin name {AuthPluginName}")]
	public static partial void ServerSentAuthPluginName(ILogger logger, string sessionId, string authPluginName);

	[LoggerMessage(EventIds.UnsupportedAuthenticationMethod, LogLevel.Error, "Session {SessionId} unsupported authentication method {AuthPluginName}")]
	public static partial void UnsupportedAuthenticationMethod(ILogger logger, string sessionId, string authPluginName);

	[LoggerMessage(EventIds.AutoDetectedAurora57, LogLevel.Debug, "Session {SessionId} auto-detected Aurora 5.7 at '{HostName}'; disabling pipelining")]
	public static partial void AutoDetectedAurora57(ILogger logger, string sessionId, string hostName);

	[LoggerMessage(EventIds.SessionMadeConnection, LogLevel.Debug, "Session {SessionId} made connection; server version {ServerVersion}; connection ID {ConnectionId}; supports: compression {SupportsCompression}, attributes {SupportsAttributes}, deprecate EOF {SupportsDeprecateEof}, cached metadata {SupportsCachedMetadata}, SSL {SupportsSsl}, session track {SupportsSessionTrack}, pipelining {SupportsPipelining}, query attributes {SupportsQueryAttributes}")]
	public static partial void SessionMadeConnection(ILogger logger, string sessionId, string serverVersion, int connectionId, bool supportsCompression, bool supportsAttributes, bool supportsDeprecateEof, bool supportsCachedMetadata, bool supportsSsl, bool supportsSessionTrack, bool supportsPipelining, bool supportsQueryAttributes);

	[LoggerMessage(EventIds.ServerDoesNotSupportSsl, LogLevel.Error, "Session {SessionId} requires SSL but server doesn't support it")]
	public static partial void ServerDoesNotSupportSsl(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.SessionDoesNotSupportSslProtocolsNone, LogLevel.Debug, "Session {SessionId} doesn't support SslProtocols.None; falling back to explicitly specifying SslProtocols")]
	public static partial void SessionDoesNotSupportSslProtocolsNone(ILogger logger, Exception exception, string sessionId);

	[LoggerMessage(EventIds.FailedNegotiatingTls, LogLevel.Debug, "Session {SessionId} failed negotiating TLS; falling back to TLS 1.1")]
	public static partial void FailedNegotiatingTls(ILogger logger, Exception exception, string sessionId);

	[LoggerMessage(EventIds.CouldNotConnectToServer, LogLevel.Error, "Session {SessionId} couldn't connect to server")]
	public static partial void CouldNotConnectToServer(ILogger logger, Exception exception, string sessionId);

	[LoggerMessage(EventIds.SendingPipelinedResetConnectionRequest, LogLevel.Trace, "Session {SessionId} server version {ServerVersion} supports reset connection and pipelining; sending pipelined reset connection request")]
	public static partial void SendingPipelinedResetConnectionRequest(ILogger logger, string sessionId, string serverVersion);

	[LoggerMessage(EventIds.SendingResetConnectionRequest, LogLevel.Trace, "Session {SessionId} server version {ServerVersion} supports reset connection; sending reset connection request")]
	public static partial void SendingResetConnectionRequest(ILogger logger, string sessionId, string serverVersion);

	[LoggerMessage(EventIds.SendingChangeUserRequest, LogLevel.Trace, "Session {SessionId} server version {ServerVersion} doesn't support reset connection; sending change user request")]
	public static partial void SendingChangeUserRequest(ILogger logger, string sessionId, string serverVersion);

	[LoggerMessage(EventIds.SendingChangeUserRequestDueToChangedDatabase, LogLevel.Debug, "Session {SessionId} sending change user request due to changed database {Database}")]
	public static partial void SendingChangeUserRequestDueToChangedDatabase(ILogger logger, string sessionId, string database);

	[LoggerMessage(EventIds.OptimisticReauthenticationFailed, LogLevel.Trace, "Session {SessionId} optimistic reauthentication failed; logging in again")]
	public static partial void OptimisticReauthenticationFailed(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.IgnoringFailureInTryResetConnectionAsync, LogLevel.Trace, "Session {SessionId} ignoring {Failure} in TryResetConnectionAsync")]
	public static partial void IgnoringFailureInTryResetConnectionAsync(ILogger logger, Exception exception, string sessionId, string failure);

	[LoggerMessage(EventIds.SwitchingToAuthenticationMethod, LogLevel.Trace, "Session {SessionId} switching to authentication method {AuthenticationMethod}")]
	public static partial void SwitchingToAuthenticationMethod(ILogger logger, string sessionId, string authenticationMethod);

	[LoggerMessage(EventIds.NeedsSecureConnection, LogLevel.Error, "Session {SessionId} needs a secure connection to use authentication method {AuthenticationMethod}")]
	public static partial void NeedsSecureConnection(ILogger logger, string sessionId, string authenticationMethod);

	[LoggerMessage(EventIds.AuthenticationMethodNotSupported, LogLevel.Error, "Session {SessionId} is requesting authentication method {AuthenticationMethod} which is not supported")]
	public static partial void AuthenticationMethodNotSupported(ILogger logger, string sessionId, string authenticationMethod);

	[LoggerMessage(EventIds.CouldNotLoadServerRsaPublicKey, LogLevel.Error, "Session {SessionId} couldn't load server's RSA public key")]
	public static partial void CouldNotLoadServerRsaPublicKey(ILogger logger, Exception exception, string sessionId);

	[LoggerMessage(EventIds.CouldNotLoadServerRsaPublicKeyFromFile, LogLevel.Error, "Session {SessionId} couldn't load server's RSA public key from '{PublicKeyFilePath}'")]
	public static partial void CouldNotLoadServerRsaPublicKeyFromFile(ILogger logger, Exception exception, string sessionId, string publicKeyFilePath);

	[LoggerMessage(EventIds.CouldNotUseAuthenticationMethodForRsa, LogLevel.Error, "Session {SessionId} couldn't use authentication method {AuthenticationMethod} because RSA key wasn't specified or couldn't be retrieved")]
	public static partial void CouldNotUseAuthenticationMethodForRsa(ILogger logger, string sessionId, string authenticationMethod);

	[LoggerMessage(EventIds.FailedToResolveHostName, LogLevel.Warning, "Session {SessionId} failed to resolve host name {HostName} ({HostNameIndex} of {HostNameCount}): {ExceptionMessage}")]
	public static partial void FailedToResolveHostName(ILogger logger, Exception exception, string sessionId, string hostName, int hostNameIndex, int hostNameCount, string exceptionMessage);

	[LoggerMessage(EventIds.ConnectingToIpAddress, LogLevel.Trace, "Session {SessionId} connecting to IP address {IpAddress} ({IpAddressIndex} of {IpAddressCount}) for host name {HostName} ({HostNameIndex} of {HostNameCount})")]
	public static partial void ConnectingToIpAddress(ILogger logger, string sessionId, string ipAddress, int ipAddressIndex, int ipAddressCount, string hostName, int hostNameIndex, int hostNameCount);

	[LoggerMessage(EventIds.ConnectTimeoutExpired, LogLevel.Information, "Session {SessionId} connect timeout expired connecting to IP address {IpAddress} for host name {HostName}")]
	public static partial void ConnectTimeoutExpired(ILogger logger, Exception? exception, string sessionId, string ipAddress, string hostName);

	[LoggerMessage(EventIds.FailedToConnectToSingleIpAddress, LogLevel.Information, "Session {SessionId} failed to connect to IP address {IpAddress} for host name {HostName}: {ExceptionMessage}")]
	public static partial void FailedToConnectToSingleIpAddress(ILogger logger, Exception exception, string sessionId, string ipAddress, string hostName, string exceptionMessage);

	[LoggerMessage(EventId = EventIds.FailedToConnectToIpAddress, Message = "Session {SessionId} failed to connect to IP address {IpAddress} ({IpAddressIndex} of {IpAddressCount}) for host name {HostName} ({HostNameIndex} of {HostNameCount}): {ExceptionMessage}")]
	public static partial void FailedToConnectToIpAddress(ILogger logger, Exception exception, LogLevel logLevel, string sessionId, string ipAddress, int ipAddressIndex, int ipAddressCount, string hostName, int hostNameIndex, int hostNameCount, string exceptionMessage);

	[LoggerMessage(EventIds.ConnectedToIpAddress, LogLevel.Trace, "Session {SessionId} connected to IP address {IpAddress} for host name {HostName} with local port {LocalPort}")]
	public static partial void ConnectedToIpAddress(ILogger logger, string sessionId, string ipAddress, string hostName, int? localPort);

	[LoggerMessage(EventIds.ConnectingToUnixSocket, LogLevel.Trace, "Session {SessionId} connecting to UNIX socket {SocketPath}")]
	public static partial void ConnectingToUnixSocket(ILogger logger, string sessionId, string socketPath);

	[LoggerMessage(EventIds.ConnectTimeoutExpiredForUnixSocket, LogLevel.Information, "Session {SessionId} connect timeout expired connecting to UNIX socket {SocketPath}")]
	public static partial void ConnectTimeoutExpiredForUnixSocket(ILogger logger, string sessionId, string socketPath);

	[LoggerMessage(EventIds.ConnectingToNamedPipe, LogLevel.Trace, "Session {SessionId} connecting to named pipe {PipeName} on server {HostName}")]
	public static partial void ConnectingToNamedPipe(ILogger logger, string sessionId, string pipeName, string hostName);

	[LoggerMessage(EventIds.ConnectTimeoutExpiredForNamedPipe, LogLevel.Information, "Session {SessionId} connect timeout expired connecting to named pipe {PipeName} on server {HostName}")]
	public static partial void ConnectTimeoutExpiredForNamedPipe(ILogger logger, Exception exception, string sessionId, string pipeName, string hostName);

	[LoggerMessage(EventIds.InitializingTlsConnection, LogLevel.Trace, "Session {SessionId} initializing TLS connection")]
	public static partial void InitializingTlsConnection(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.NoCertificatesFound, LogLevel.Error, "Session {SessionId} found no certificates in the certificate store")]
	public static partial void NoCertificatesFound(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.CertificateNotFoundInStore, LogLevel.Error, "Session {SessionId} certificate with thumbprint {Thumbprint} not found in store")]
	public static partial void CertificateNotFoundInStore(ILogger logger, string sessionId, string thumbprint);

	[LoggerMessage(EventIds.CouldNotLoadCertificate, LogLevel.Error, "Session {SessionId} couldn't load certificate from {CertificateStoreLocation}")]
	public static partial void CouldNotLoadCertificate(ILogger logger, Exception exception, string sessionId, MySqlCertificateStoreLocation certificateStoreLocation);

	[LoggerMessage(EventIds.NoPrivateKeyIncludedWithCertificateFile, LogLevel.Error, "Session {SessionId} no private key included with certificate file '{CertificateFile}'")]
	public static partial void NoPrivateKeyIncludedWithCertificateFile(ILogger logger, string sessionId, string certificateFile);

	[LoggerMessage(EventIds.CouldNotLoadCertificateFromFile, LogLevel.Error, "Session {SessionId} couldn't load certificate from '{CertificateFile}'")]
	public static partial void CouldNotLoadCertificateFromFile(ILogger logger, Exception exception, string sessionId, string certificateFile);

	[LoggerMessage(EventIds.FailedToObtainClientCertificates, LogLevel.Error, "Session {SessionId} failed to obtain client certificates via ProvideClientCertificatesCallback: {ExceptionMessage}")]
	public static partial void FailedToObtainClientCertificates(ILogger logger, Exception exception, string sessionId, string exceptionMessage);

	[LoggerMessage(EventIds.LoadingCaCertificatesFromFile, LogLevel.Trace, "Session {SessionId} loading CA certificate(s) from '{CACertificateFile}'")]
	public static partial void LoadingCaCertificatesFromFile(ILogger logger, string sessionId, string caCertificateFile);

	[LoggerMessage(EventId = EventIds.CouldNotLoadCaCertificateFromFile, Message = "Session {SessionId} couldn't load CA certificate from '{CACertificateFile}'")]
	public static partial void CouldNotLoadCaCertificateFromFile(ILogger logger, Exception exception, LogLevel logLevel, string sessionId, string caCertificateFile);

	[LoggerMessage(EventIds.LoadingCaCertificate, LogLevel.Trace, "Session {SessionId} loading certificate at index {Index} in the CA certificate file.")]
	public static partial void LoadingCaCertificate(ILogger logger, string sessionId, int index);

	[LoggerMessage(EventIds.LoadedCaCertificatesFromFile, LogLevel.Trace, "Session {SessionId} loaded {CertificateCount} certificate(s) from '{CACertificateFile}'")]
	public static partial void LoadedCaCertificatesFromFile(ILogger logger, string sessionId, int certificateCount, string caCertificateFile);

	[LoggerMessage(EventIds.NotUsingRemoteCertificateValidationCallbackDueToSslCa, LogLevel.Warning, "Session {SessionId} not using client-provided RemoteCertificateValidationCallback because SslCA is specified")]
	public static partial void NotUsingRemoteCertificateValidationCallbackDueToSslCa(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.NotUsingRemoteCertificateValidationCallbackDueToSslMode, LogLevel.Warning, "Session {SessionId} not using client-provided RemoteCertificateValidationCallback because SslMode is {SslMode}")]
	public static partial void NotUsingRemoteCertificateValidationCallbackDueToSslMode(ILogger logger, string sessionId, MySqlSslMode sslMode);

	[LoggerMessage(EventIds.UsingRemoteCertificateValidationCallback, LogLevel.Debug, "Session {SessionId} using client-provided RemoteCertificateValidationCallback")]
	public static partial void UsingRemoteCertificateValidationCallback(ILogger logger, string sessionId);

#if NETCOREAPP3_0_OR_GREATER
	[LoggerMessage(EventIds.ConnectedTlsBasic, LogLevel.Debug, "Session {SessionId} connected TLS using {SslProtocol} and {NegotiatedCipherSuite}")]
	public static partial void ConnectedTlsBasic(ILogger logger, string sessionId, SslProtocols sslProtocol, TlsCipherSuite negotiatedCipherSuite);
#endif

	[LoggerMessage(EventIds.ConnectedTlsDetailed, LogLevel.Debug, "Session {SessionId} connected TLS using {SslProtocol}, {CipherAlgorithm}, {HashAlgorithm}, {KeyExchangeAlgorithm}, {KeyExchangeStrength}")]
	public static partial void ConnectedTlsDetailed(ILogger logger, string sessionId, SslProtocols sslProtocol, CipherAlgorithmType cipherAlgorithm, HashAlgorithmType hashAlgorithm, ExchangeAlgorithmType keyExchangeAlgorithm, int keyExchangeStrength);

	[LoggerMessage(EventIds.CouldNotInitializeTlsConnection, LogLevel.Error, "Session {SessionId} couldn't initialize TLS connection")]
	public static partial void CouldNotInitializeTlsConnection(ILogger logger, Exception exception, string sessionId);

	[LoggerMessage(EventIds.LoadingClientKeyFromKeyFile, LogLevel.Trace, "Session {SessionId} loading client key from '{ClientKeyFilePath}'")]
	public static partial void LoadingClientKeyFromKeyFile(ILogger logger, string sessionId, string clientKeyFilePath);

	[LoggerMessage(EventIds.CouldNotLoadClientKeyFromKeyFile, LogLevel.Error, "Session {SessionId} couldn't load client key from '{ClientKeyFilePath}'")]
	public static partial void CouldNotLoadClientKeyFromKeyFile(ILogger logger, Exception exception, string sessionId, string clientKeyFilePath);

	[LoggerMessage(EventIds.DetectedProxy, LogLevel.Debug, "Session {SessionId} detected proxy; getting CONNECTION_ID(), VERSION() from server")]
	public static partial void DetectedProxy(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.ChangingConnectionId, LogLevel.Debug, "Session {SessionId} changing connection id from {OldConnectionId} to {ConnectionId} and server version from {OldServerVersion} to {ServerVersion}")]
	public static partial void ChangingConnectionId(ILogger logger, string sessionId, int oldConnectionId, int connectionId, string oldServerVersion, string serverVersion);

	[LoggerMessage(EventIds.FailedToGetConnectionId, LogLevel.Information, "Session {SessionId} failed to get CONNECTION_ID(), VERSION()")]
	public static partial void FailedToGetConnectionId(ILogger logger, Exception exception, string sessionId);

	[LoggerMessage(EventIds.ClosingStreamSocket, LogLevel.Debug, "Session {SessionId} closing stream/socket")]
	public static partial void ClosingStreamSocket(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.CreatingConnectionAttributes, LogLevel.Trace, "Session {SessionId} creating connection attributes")]
	public static partial void CreatingConnectionAttributes(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.ObtainingPasswordViaProvidePasswordCallback, LogLevel.Trace, "Session {SessionId} obtaining password via ProvidePasswordCallback")]
	public static partial void ObtainingPasswordViaProvidePasswordCallback(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.FailedToObtainPassword, LogLevel.Error, "Session {SessionId} failed to obtain password via ProvidePasswordCallback: {ExceptionMessage}")]
	public static partial void FailedToObtainPassword(ILogger logger, Exception exception, string sessionId, string exceptionMessage);

	[LoggerMessage(EventIds.IgnoringCancellationForCommand, LogLevel.Trace, "Ignoring cancellation for closed connection or invalid command {CommandId}")]
	public static partial void IgnoringCancellationForCommand(ILogger logger, int commandId);

	[LoggerMessage(EventIds.CommandHasBeenCanceled, LogLevel.Debug, "Command {CommandId} for session {SessionId} has been canceled via {CancellationSource}")]
	public static partial void CommandHasBeenCanceled(ILogger logger, int commandId, string sessionId, string cancellationSource);

	[LoggerMessage(EventIds.IgnoringCancellationForClosedConnection, LogLevel.Information, "Session {SessionId} ignoring cancellation for closed connection")]
	public static partial void IgnoringCancellationForClosedConnection(ILogger logger, Exception exception, string sessionId);

	[LoggerMessage(EventIds.CancelingCommandFailed, LogLevel.Information, "Session {SessionId} cancelling command {CommandId} failed")]
	public static partial void CancelingCommandFailed(ILogger logger, Exception exception, string sessionId, int commandId);

	[LoggerMessage(EventIds.WillCancelCommand, LogLevel.Debug, "Session {SessionId} will cancel command {CommandId} ({CancelAttemptCount} attempts); CommandText: {CommandText}")]
	public static partial void WillCancelCommand(ILogger logger, string sessionId, int commandId, int cancelAttemptCount, string? commandText);

	[LoggerMessage(EventIds.CancelingCommandFromSession, LogLevel.Information, "Session {SessionId} canceling command {CommandId} from session {CancelingSessionId}; CommandText: {CommandText}")]
	public static partial void CancelingCommandFromSession(ILogger logger, string sessionId, int commandId, string cancelingSessionId, string? commandText);

	[LoggerMessage(EventIds.IgnoringCancellationForInactiveCommand, LogLevel.Debug, "Session {SessionId} active command {ActiveCommandId} is not the command {CommandId} being canceled; ignoring cancellation.")]
	public static partial void IgnoringCancellationForInactiveCommand(ILogger logger, string sessionId, int activeCommandId, int commandId);

	[LoggerMessage(EventIds.CancelingCommand, LogLevel.Debug, "Session {SessionId} canceling command {CommandId} with text {CommandText}")]
	public static partial void CancelingCommand(ILogger logger, string sessionId, int commandId, string commandText);

	[LoggerMessage(EventIds.SendingSleepToClearPendingCancellation, LogLevel.Debug, "Session {SessionId} sending 'SLEEP(0)' command to clear pending cancellation")]
	public static partial void SendingSleepToClearPendingCancellation(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.GettingCachedProcedure, LogLevel.Trace, "Session {SessionId} getting cached procedure named {ProcedureName}")]
	public static partial void GettingCachedProcedure(ILogger logger, string sessionId, string procedureName);

	[LoggerMessage(EventIds.PoolDoesNotHaveSharedProcedureCache, LogLevel.Information, "Session {SessionId} pool {PoolId} doesn't have a shared procedure cache; procedure will only be cached on this connection")]
	public static partial void PoolDoesNotHaveSharedProcedureCache(ILogger logger, string sessionId, int? poolId);

	[LoggerMessage(EventIds.CouldNotNormalizeDatabaseAndName, LogLevel.Information, "Session {SessionId} couldn't normalize the name '{ProcedureName}' in database {Database}; not caching procedure")]
	public static partial void CouldNotNormalizeDatabaseAndName(ILogger logger, string sessionId, string procedureName, string database);

	[LoggerMessage(EventIds.FailedToCacheProcedure, LogLevel.Information, "Session {SessionId} failed to cache procedure {Schema}.{Component}")]
	public static partial void FailedToCacheProcedure(ILogger logger, string sessionId, string schema, string component);

	[LoggerMessage(EventIds.CachingProcedure, LogLevel.Trace, "Session {SessionId} caching procedure {Schema}.{Component}")]
	public static partial void CachingProcedure(ILogger logger, string sessionId, string schema, string component);

	[LoggerMessage(EventIds.ProcedureCacheCount, LogLevel.Trace, "Session {SessionId} procedure cache count is {ProcedureCacheCount}")]
	public static partial void ProcedureCacheCount(ILogger logger, string sessionId, int procedureCacheCount);

	[LoggerMessage(EventIds.DidNotFindCachedProcedure, LogLevel.Information, "Session {SessionId} did not find cached procedure {Schema}.{Component}")]
	public static partial void DidNotFindCachedProcedure(ILogger logger, string sessionId, string schema, string component);

	[LoggerMessage(EventIds.ReturningCachedProcedure, LogLevel.Trace, "Session {SessionId} returning cached procedure {Schema}.{Component}")]
	public static partial void ReturningCachedProcedure(ILogger logger, string sessionId, string schema, string component);

	[LoggerMessage(EventIds.FailedToRetrieveProcedureMetadata, LogLevel.Information, "Session {SessionId} failed to retrieve metadata for {Schema}.{Component}; falling back to INFORMATION_SCHEMA: {ExceptionMessage}")]
	public static partial void FailedToRetrieveProcedureMetadata(ILogger logger, Exception exception, string sessionId, string schema, string component, string exceptionMessage);

	[LoggerMessage(EventIds.ServerDoesNotSupportCachedProcedures, LogLevel.Information, "Session {SessionId} server version {ServerVersion} does not support cached procedures")]
	public static partial void ServerDoesNotSupportCachedProcedures(ILogger logger, string sessionId, string serverVersion);

	[LoggerMessage(EventIds.ProcedureHasRoutineCount, LogLevel.Trace, "Procedure for {Schema}.{Component} has {RoutineCount} routines and {ParameterCount} parameters")]
	public static partial void ProcedureHasRoutineCount(ILogger logger, string schema, string component, int routineCount, int parameterCount);

	[LoggerMessage(EventIds.CreatedNewSession, LogLevel.Trace, "Created new session {SessionId}")]
	public static partial void CreatedNewSession(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.PingingServer, LogLevel.Trace, "Session {SessionId} pinging server")]
	public static partial void PingingServer(ILogger logger, string sessionId);

	[LoggerMessage(EventId = EventIds.SuccessfullyPingedServer, Message = "Session {SessionId} successfully pinged server")]
	public static partial void SuccessfullyPingedServer(ILogger logger, LogLevel logLevel, string sessionId);

	[LoggerMessage(EventIds.PingFailed, LogLevel.Trace, "Session {SessionId} ping failed due to {Failure}")]
	public static partial void PingFailed(ILogger logger, Exception exception, string sessionId, string failure);

	[LoggerMessage(EventIds.SettingStateToFailed, LogLevel.Debug, "Session {SessionId} setting state to Failed")]
	public static partial void SettingStateToFailed(ILogger logger, Exception exception, string sessionId);

	[LoggerMessage(EventIds.ErrorPayload, LogLevel.Debug, "Session {SessionId} got error payload: {ErrorCode}, {State}, {Message}")]
	public static partial void ErrorPayload(ILogger logger, string sessionId, int errorCode, string state, string message);

	[LoggerMessage(EventIds.CommandExecutorExecuteReader, LogLevel.Trace, "Session {SessionId} ExecuteReader {IOBehavior} for {CommandCount} command(s)")]
	public static partial void CommandExecutorExecuteReader(ILogger logger, string sessionId, IOBehavior ioBehavior, int commandCount);

	[LoggerMessage(EventIds.QueryWasInterrupted, LogLevel.Information, "Session {SessionId} query was interrupted")]
	public static partial void QueryWasInterrupted(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.PreparingCommandPayload, LogLevel.Trace, "Session {SessionId} preparing command payload for: {CommandText}")]
	public static partial void PreparingCommandPayload(ILogger logger, string sessionId, string commandText);

	[LoggerMessage(EventIds.PreparingCommandPayloadWithId, LogLevel.Trace, "Session {SessionId} preparing statement payload with ID {StatementId} for: {CommandText}")]
	public static partial void PreparingCommandPayloadWithId(ILogger logger, string sessionId, int statementId, string commandText);

	[LoggerMessage(EventIds.QueryAttributesNotSupported, LogLevel.Warning, "Session {SessionId} has query attributes but server doesn't support them; CommandText: {CommandText}")]
	public static partial void QueryAttributesNotSupported(ILogger logger, string sessionId, string commandText);

	[LoggerMessage(EventIds.QueryAttributesNotSupportedWithId, LogLevel.Warning, "Session {SessionId} has attributes for statement {StatementId} but the server does not support them")]
	public static partial void QueryAttributesNotSupportedWithId(ILogger logger, string sessionId, int statementId);

	[LoggerMessage(EventIds.IgnoringExceptionInDisposeAsync, LogLevel.Warning, "Session {SessionId} ignoring exception in MySqlDataReader.DisposeAsync. Message: {ExceptionMessage}. CommandText: {CommandText}")]
	public static partial void IgnoringExceptionInDisposeAsync(ILogger logger, Exception exception, string sessionId, string exceptionMessage, string commandText);

	[LoggerMessage(EventIds.StartingBulkCopy, LogLevel.Debug, "Starting bulk copy to {TableName}")]
	public static partial void StartingBulkCopy(ILogger logger, string tableName);

	[LoggerMessage(EventIds.AddingDefaultColumnMapping, LogLevel.Debug, "Adding default column mapping from {SourceOrdinal} to {DestinationColumn}")]
	public static partial void AddingDefaultColumnMapping(ILogger logger, int sourceOrdinal, string destinationColumn);

	[LoggerMessage(EventIds.IgnoringColumn, LogLevel.Debug, "Ignoring column with source ordinal {SourceOrdinal}")]
	public static partial void IgnoringColumn(ILogger logger, int sourceOrdinal);

	[LoggerMessage(EventIds.FinishedBulkCopy, LogLevel.Debug, "Finished bulk copy to {TableName}")]
	public static partial void FinishedBulkCopy(ILogger logger, string tableName);

	[LoggerMessage(EventIds.BulkCopyFailed, LogLevel.Error, "Bulk copy to {TableName} failed: {RowsCopied} row(s) copied; {RowsInserted} row(s) inserted")]
	public static partial void BulkCopyFailed(ILogger logger, string tableName, int rowsCopied, int rowsInserted);

	[LoggerMessage(EventIds.ColumnMappingAlreadyHasExpression, LogLevel.Information, "Column mapping for {SourceOrdinal} to {DestinationColumn} already has expression {Expression}")]
	public static partial void ColumnMappingAlreadyHasExpression(ILogger logger, int sourceOrdinal, string destinationColumn, string expression);

	[LoggerMessage(EventIds.SettingExpressionToMapColumn, LogLevel.Trace, "Setting expression to map column {SourceOrdinal} to {DestinationColumn}: {Expression}")]
	public static partial void SettingExpressionToMapColumn(ILogger logger, int sourceOrdinal, string destinationColumn, string expression);

	[LoggerMessage(EventIds.WaitingForAvailableSession, LogLevel.Trace, "Pool {PoolId} waiting for an available session")]
	public static partial void WaitingForAvailableSession(ILogger logger, int poolId);

	[LoggerMessage(EventIds.FailedInReceiveReplyAsync, LogLevel.Debug, "Session {SessionId} failed in ReceiveReplyAsync")]
	public static partial void FailedInReceiveReplyAsync(ILogger logger, Exception exception, string sessionId);

	[LoggerMessage(EventIds.FailedInSendReplyAsync, LogLevel.Debug, "Session {SessionId} failed in SendReplyAsync")]
	public static partial void FailedInSendReplyAsync(ILogger logger, Exception exception, string sessionId);

	[LoggerMessage(EventIds.CreatingNewConnectionPool, LogLevel.Information, "Creating new connection pool {PoolId} for {ConnectionString}")]
	public static partial void CreatingNewConnectionPool(ILogger logger, int poolId, string connectionString);

	[LoggerMessage(EventIds.ScanningForLeakedSessions, LogLevel.Debug, "Pool {PoolId} is empty; scanning for any leaked sessions")]
	public static partial void ScanningForLeakedSessions(ILogger logger, int poolId);

	[LoggerMessage(EventIds.FoundExistingSession, LogLevel.Trace, "Pool {PoolId} found an existing session; checking it for validity")]
	public static partial void FoundExistingSession(ILogger logger, int poolId);

	[LoggerMessage(EventIds.DiscardingSessionDueToWrongGeneration, LogLevel.Trace, "Pool {PoolId} discarding session due to wrong generation")]
	public static partial void DiscardingSessionDueToWrongGeneration(ILogger logger, int poolId);

	[LoggerMessage(EventIds.SessionIsUnusable, LogLevel.Information, "Pool {PoolId} session {SessionId} is unusable; destroying it")]
	public static partial void SessionIsUnusable(ILogger logger, int poolId, string sessionId);

	[LoggerMessage(EventIds.ReturningPooledSession, LogLevel.Trace, "Pool {PoolId} returning pooled session {SessionId} to caller; {LeasedSessionCount} leased session(s)")]
	public static partial void ReturningPooledSession(ILogger logger, int poolId, string sessionId, int leasedSessionCount);

	[LoggerMessage(EventIds.ReturningNewSession, LogLevel.Trace, "Pool {PoolId} returning new session {SessionId} to caller; {LeasedSessionCount} leased session(s)")]
	public static partial void ReturningNewSession(ILogger logger, int poolId, string sessionId, int leasedSessionCount);

	[LoggerMessage(EventIds.DisposingCreatedSessionDueToException, LogLevel.Debug, "Pool {PoolId} disposing created session {SessionId} due to exception: {ExceptionMessage}")]
	public static partial void DisposingCreatedSessionDueToException(ILogger logger, Exception exception, int poolId, string sessionId, string exceptionMessage);

	[LoggerMessage(EventIds.UnexpectedErrorInGetSessionAsync, LogLevel.Warning, "Pool {PoolId} unexpected error in GetSessionAsync: {ExceptionMessage}")]
	public static partial void UnexpectedErrorInGetSessionAsync(ILogger logger, Exception exception, int poolId, string exceptionMessage);

	[LoggerMessage(EventIds.ReceivingSessionBack, LogLevel.Trace, "Pool {PoolId} receiving session {SessionId} back")]
	public static partial void ReceivingSessionBack(ILogger logger, int poolId, string sessionId);

	[LoggerMessage(EventIds.ReceivedInvalidSession, LogLevel.Information, "Pool {PoolId} received invalid session {SessionId}; destroying it")]
	public static partial void ReceivedInvalidSession(ILogger logger, int poolId, string sessionId);

	[LoggerMessage(EventIds.ReceivedExpiredSession, LogLevel.Debug, "Pool {PoolId} received expired session {SessionId}; destroying it")]
	public static partial void ReceivedExpiredSession(ILogger logger, int poolId, string sessionId);

	[LoggerMessage(EventIds.ClearingConnectionPool, LogLevel.Information, "Pool {PoolId} clearing connection pool")]
	public static partial void ClearingConnectionPool(ILogger logger, int poolId);

	[LoggerMessage(EventIds.DisposingConnectionPool, LogLevel.Debug, "Pool {PoolId} disposing connection pool")]
	public static partial void DisposingConnectionPool(ILogger logger, int poolId);

	[LoggerMessage(EventIds.RecoveredNoSessions, LogLevel.Trace, "Pool {PoolId} recovered no sessions")]
	public static partial void RecoveredNoSessions(ILogger logger, int poolId);

	[LoggerMessage(EventIds.RecoveredSessionCount, LogLevel.Warning, "Pool {PoolId} recovered {SessionCount} sessions")]
	public static partial void RecoveredSessionCount(ILogger logger, int poolId, int sessionCount);

	[LoggerMessage(EventIds.FoundSessionToCleanUp, LogLevel.Debug, "Pool {PoolId} found session {SessionId} to clean up")]
	public static partial void FoundSessionToCleanUp(ILogger logger, int poolId, string sessionId);

	[LoggerMessage(EventIds.HasServerRedirectionHeader, LogLevel.Trace, "Session {SessionId} has server redirection header {Header}")]
	public static partial void HasServerRedirectionHeader(ILogger logger, string sessionId, string header);

	[LoggerMessage(EventIds.ServerRedirectionIsDisabled, LogLevel.Trace, "Pool {PoolId} server redirection is disabled; ignoring redirection")]
	public static partial void ServerRedirectionIsDisabled(ILogger logger, int poolId);

	[LoggerMessage(EventIds.OpeningNewConnection, LogLevel.Debug, "Pool {PoolId} opening new connection to {Host}:{Port} as {User}")]
	public static partial void OpeningNewConnection(ILogger logger, int poolId, string host, int port, string user);

	[LoggerMessage(EventIds.FailedToConnectRedirectedSession, LogLevel.Information, "Pool {PoolId} failed to connect redirected session {SessionId}")]
	public static partial void FailedToConnectRedirectedSession(ILogger logger, Exception ex, int poolId, string sessionId);

	[LoggerMessage(EventIds.ClosingSessionToUseRedirectedSession, LogLevel.Trace, "Pool {PoolId} closing session {SessionId} to use redirected session {RedirectedSessionId} instead")]
	public static partial void ClosingSessionToUseRedirectedSession(ILogger logger, int poolId, string sessionId, string redirectedSessionId);

	[LoggerMessage(EventIds.SessionAlreadyConnectedToServer, LogLevel.Trace, "Session {SessionId} is already connected to this server; ignoring redirection")]
	public static partial void SessionAlreadyConnectedToServer(ILogger logger, string sessionId);

	[LoggerMessage(EventIds.RequiresServerRedirection, LogLevel.Error, "Pool {PoolId} requires server redirection but server doesn't support it")]
	public static partial void RequiresServerRedirection(ILogger logger, int poolId);

	[LoggerMessage(EventIds.CreatedPoolWillNotBeUsed, LogLevel.Debug, "Pool {PoolId} was created but will not be used (due to race)")]
	public static partial void CreatedPoolWillNotBeUsed(ILogger logger, int poolId);

	[LoggerMessage(EventIds.ReapingConnectionPool, LogLevel.Trace, "Pool {PoolId} reaping connection pool")]
	public static partial void ReapingConnectionPool(ILogger logger, int poolId);

	[LoggerMessage(EventIds.CheckingForDnsChanges, LogLevel.Trace, "Pool {PoolId} checking for DNS changes")]
	public static partial void CheckingForDnsChanges(ILogger logger, int poolId);

	[LoggerMessage(EventIds.DetectedDnsChange, LogLevel.Debug, "Pool {PoolId} detected DNS change for '{HostName}': {OldAddresses} to {NewAddresses}")]
	public static partial void DetectedDnsChange(ILogger logger, int poolId, string hostName, string oldAddresses, string newAddresses);

	[LoggerMessage(EventIds.DnsCheckFailed, LogLevel.Debug, "Pool {PoolId} DNS check failed; ignoring '{HostName}': {ExceptionMessage}")]
	public static partial void DnsCheckFailed(ILogger logger, Exception exception, int poolId, string hostName, string exceptionMessage);

	[LoggerMessage(EventIds.ClearingPoolDueToDnsChanges, LogLevel.Information, "Pool {PoolId} clearing pool due to DNS changes")]
	public static partial void ClearingPoolDueToDnsChanges(ILogger logger, int poolId);
}
