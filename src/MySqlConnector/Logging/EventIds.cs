namespace MySqlConnector.Logging;

internal static class EventIds
{
	// DataSource events, 1000-1099
	public const int DataSourceCreatedWithPoolWithName = 1000;
	public const int DataSourceCreatedWithoutPoolWithName = 1001;
	public const int DataSourceCreatedWithPoolWithoutName = 1002;
	public const int DataSourceCreatedWithoutPoolWithoutName = 1003;

	// DataSource errors, 1100-1199
	public const int PeriodicPasswordProviderFailed = 1100;

	// Core Session events, 2000-2099
	public const int CreatedNewSession = 2000;
	public const int CreatedNonPooledSession = 2001;
	public const int ResettingConnection = 2002;
	public const int ReturningToPool = 2003;
	public const int SendingQuitCommand = 2004;
	public const int ClosingStreamSocket = 2005;
	public const int ErrorPayload = 2006;
	public const int FailedInSendReplyAsync = 2007;
	public const int FailedInReceiveReplyAsync = 2008;
	public const int SettingStateToFailed = 2009;
	public const int ExpectedSessionState1 = 2011;
	public const int ExpectedSessionState6 = 2016;

	// Session connecting events, 2100-2199
	public const int ConnectingFailed = 2100;
	public const int ServerSentAuthPluginName = 2101;
	public const int UnsupportedAuthenticationMethod = 2102;
	public const int AutoDetectedAurora57 = 2103;
	public const int SessionMadeConnection = 2104;
	public const int ServerDoesNotSupportSsl = 2105;
	public const int SessionDoesNotSupportSslProtocolsNone = 2106;
	public const int FailedNegotiatingTls = 2107;
	public const int CouldNotConnectToServer = 2108;
	public const int SendingPipelinedResetConnectionRequest = 2109;
	public const int SendingResetConnectionRequest = 2110;
	public const int SendingChangeUserRequest = 2111;
	public const int SendingChangeUserRequestDueToChangedDatabase = 2112;
	public const int OptimisticReauthenticationFailed = 2113;
	public const int IgnoringFailureInTryResetConnectionAsync = 2114;
	public const int SwitchingToAuthenticationMethod = 2115;
	public const int NeedsSecureConnection = 2116;
	public const int AuthenticationMethodNotSupported = 2117;
	public const int CouldNotLoadServerRsaPublicKey = 2118;
	public const int CouldNotLoadServerRsaPublicKeyFromFile = 2119;
	public const int CouldNotUseAuthenticationMethodForRsa = 2120;
	public const int FailedToResolveHostName = 2121;
	public const int ConnectingToIpAddress = 2122;
	public const int ConnectTimeoutExpired = 2123;
	public const int FailedToConnectToSingleIpAddress = 2124;
	public const int FailedToConnectToIpAddress = 2125;
	public const int ConnectedToIpAddress = 2126;
	public const int ConnectingToUnixSocket = 2127;
	public const int ConnectTimeoutExpiredForUnixSocket = 2128;
	public const int ConnectingToNamedPipe = 2129;
	public const int ConnectTimeoutExpiredForNamedPipe = 2130;
	public const int InitializingTlsConnection = 2131;
	public const int NoCertificatesFound = 2132;
	public const int CertificateNotFoundInStore = 2133;
	public const int CouldNotLoadCertificate = 2134;
	public const int NoPrivateKeyIncludedWithCertificateFile = 2135;
	public const int CouldNotLoadCertificateFromFile = 2136;
	public const int FailedToObtainClientCertificates = 2137;
	public const int LoadingCaCertificatesFromFile = 2138;
	public const int CouldNotLoadCaCertificateFromFile = 2139;
	public const int LoadingCaCertificate = 2140;
	public const int LoadedCaCertificatesFromFile = 2141;
	public const int NotUsingRemoteCertificateValidationCallbackDueToSslCa = 2142;
	public const int NotUsingRemoteCertificateValidationCallbackDueToSslMode = 2143;
	public const int UsingRemoteCertificateValidationCallback = 2144;
	public const int ConnectedTlsBasic = 2145;
	public const int ConnectedTlsDetailed = 2146;
	public const int CouldNotInitializeTlsConnection = 2147;
	public const int LoadingClientKeyFromKeyFile = 2148;
	public const int CouldNotLoadClientKeyFromKeyFile = 2149;
	public const int DetectedProxy = 2150;
	public const int ChangingConnectionId = 2151;
	public const int FailedToGetConnectionId = 2152;
	public const int CreatingConnectionAttributes = 2153;
	public const int ObtainingPasswordViaProvidePasswordCallback = 2154;
	public const int FailedToObtainPassword = 2155;

	// Command execution events, 2200-2299
	public const int CannotExecuteNewCommandInState = 2200;
	public const int EnteringFinishQuerying = 2201;
	public const int CommandExecutorExecuteReader = 2202;
	public const int QueryWasInterrupted = 2203;
	public const int PreparingCommandPayload = 2204;
	public const int PreparingCommandPayloadWithId = 2205;
	public const int QueryAttributesNotSupported = 2206;
	public const int QueryAttributesNotSupportedWithId = 2207;
	public const int IgnoringExceptionInDisposeAsync = 2208;

	// Command cancellation events, 2300-2399
	public const int IgnoringCancellationForCommand = 2300;
	public const int CommandHasBeenCanceled = 2301;
	public const int IgnoringCancellationForClosedConnection = 2302;
	public const int CancelingCommandFailed = 2303;
	public const int WillCancelCommand = 2304;
	public const int CancelingCommandFromSession = 2305;
	public const int IgnoringCancellationForInactiveCommand = 2306;
	public const int CancelingCommand = 2307;
	public const int SendingSleepToClearPendingCancellation = 2308;

	// Cached procedure events, 2400-2499
	public const int GettingCachedProcedure = 2400;
	public const int PoolDoesNotHaveSharedProcedureCache = 2401;
	public const int CouldNotNormalizeDatabaseAndName = 2402;
	public const int FailedToCacheProcedure = 2403;
	public const int CachingProcedure = 2404;
	public const int ProcedureCacheCount = 2405;
	public const int DidNotFindCachedProcedure = 2406;
	public const int ReturningCachedProcedure = 2407;
	public const int FailedToRetrieveProcedureMetadata = 2408;
	public const int ServerDoesNotSupportCachedProcedures = 2409;
	public const int ProcedureHasRoutineCount = 2410;

	// Ping events, 2500-2599
	public const int PingingServer = 2500;
	public const int SuccessfullyPingedServer = 2501;
	public const int PingFailed = 2502;

	// Bulk copy events, 2600-2699
	public const int StartingBulkCopy = 2503;
	public const int AddingDefaultColumnMapping = 2504;
	public const int IgnoringColumn = 2505;
	public const int FinishedBulkCopy = 2506;
	public const int BulkCopyFailed = 2507;
	public const int ColumnMappingAlreadyHasExpression = 2508;
	public const int SettingExpressionToMapColumn = 2509;

	// Connection pool events, 3000-3099
	public const int WaitingForAvailableSession = 3000;
	public const int CreatingNewConnectionPool = 3001;
	public const int ScanningForLeakedSessions = 3002;
	public const int FoundExistingSession = 3003;
	public const int DiscardingSessionDueToWrongGeneration = 3004;
	public const int SessionIsUnusable = 3005;
	public const int ReturningPooledSession = 3006;
	public const int ReturningNewSession = 3007;
	public const int DisposingCreatedSessionDueToException = 3008;
	public const int UnexpectedErrorInGetSessionAsync = 3009;
	public const int ReceivingSessionBack = 3010;
	public const int ReceivedInvalidSession = 3011;
	public const int ReceivedExpiredSession = 3012;
	public const int ClearingConnectionPool = 3013;
	public const int DisposingConnectionPool = 3014;
	public const int RecoveredNoSessions = 3015;
	public const int RecoveredSessionCount = 3016;
	public const int FoundSessionToCleanUp = 3017;
	public const int PoolCreatedNewSession = 3018;
	public const int CreatedSessionToReachMinimumPoolCount = 3019;
	public const int CreatedPoolWillNotBeUsed = 3020;

	// Connection pool background events, 3100-3199
	public const int ReapingConnectionPool = 3100;
	public const int CheckingForDnsChanges = 3101;
	public const int DetectedDnsChange = 3102;
	public const int DnsCheckFailed = 3103;
	public const int ClearingPoolDueToDnsChanges = 3104;

	// Server redirection events, 3200-3299
	public const int HasServerRedirectionHeader = 3200;
	public const int ServerRedirectionIsDisabled = 3201;
	public const int OpeningNewConnection = 3202;
	public const int FailedToConnectRedirectedSession = 3203;
	public const int ClosingSessionToUseRedirectedSession = 3204;
	public const int SessionAlreadyConnectedToServer = 3205;
	public const int RequiresServerRedirection = 3206;
}
