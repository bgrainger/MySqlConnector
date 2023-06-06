using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using MySqlConnector.Core;
using MySqlConnector.Logging;
using MySqlConnector.Protocol.Serialization;

namespace MySqlConnector;

/// <summary>
/// <see cref="MySqlDataSource"/> implements a MySQL data source which can be used to obtain open connections, and against which commands can be executed directly.
/// </summary>
public sealed class MySqlDataSource : DbDataSource
{
	/// <summary>
	/// Initializes a new instance of the <see cref="MySqlDataSource"/> class.
	/// </summary>
	/// <param name="connectionString">The connection string for the MySQL Server. This parameter is required.</param>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="connectionString"/> is <c>null</c>.</exception>
	public MySqlDataSource(string connectionString)
		: this(connectionString ?? throw new ArgumentNullException(nameof(connectionString)), MySqlConnectorLoggingConfiguration.NullConfiguration, null, null, null, null, default, default)
	{
	}

	internal MySqlDataSource(string connectionString,
		MySqlConnectorLoggingConfiguration loggingConfiguration,
		string? name,
		Func<X509CertificateCollection, ValueTask>? clientCertificatesCallback,
		RemoteCertificateValidationCallback? remoteCertificateValidationCallback,
		Func<MySqlProvidePasswordContext, CancellationToken, ValueTask<string>>? periodicPasswordProvider,
		TimeSpan periodicPasswordProviderSuccessRefreshInterval,
		TimeSpan periodicPasswordProviderFailureRefreshInterval)
	{
		m_connectionString = connectionString;
		LoggingConfiguration = loggingConfiguration;
		Name = name;
		m_clientCertificatesCallback = clientCertificatesCallback;
		m_remoteCertificateValidationCallback = remoteCertificateValidationCallback;
		m_logger = loggingConfiguration.DataSourceLogger;

		Pool = ConnectionPool.CreatePool(m_connectionString, LoggingConfiguration, name);
		m_id = Interlocked.Increment(ref s_lastId);
		if (Pool is not null && Name is not null)
			Log.DataSourceCreatedWithPoolWithName(m_logger, m_id, Pool.Id, Name);
		else if (Pool is not null)
			Log.DataSourceCreatedWithPoolWithoutName(m_logger, m_id, Pool.Id);
		else if (Name is not null)
			Log.DataSourceCreatedWithoutPoolWithName(m_logger, m_id, Name);
		else
			Log.DataSourceCreatedWithoutPoolWithoutName(m_logger, m_id);

		if (periodicPasswordProvider is not null)
		{
			m_periodicPasswordProvider = periodicPasswordProvider;
			m_periodicPasswordProviderSuccessRefreshInterval = periodicPasswordProviderSuccessRefreshInterval;
			m_periodicPasswordProviderFailureRefreshInterval = periodicPasswordProviderFailureRefreshInterval;

			m_passwordProviderTimerCancellationTokenSource = new();
			var csb = new MySqlConnectionStringBuilder(m_connectionString);
			m_providePasswordContext = new(csb.Server, (int) csb.Port, csb.UserID, csb.Database);

			// create the timer but don't start it; the manual run below will will schedule the first refresh
			// see https://github.com/davidfowl/AspNetCoreDiagnosticScenarios/blob/master/AsyncGuidance.md#timer-callbacks for code pattern
			m_passwordProviderTimer = new Timer(_ => _ = RefreshPassword(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

			// trigger the first refresh attempt right now, outside the timer; this allows us to capture the Task so it can be observed in ComplexGetPassword
			m_initialPasswordRefreshTask = Task.Run(RefreshPassword);
			m_providePasswordCallback = ProvidePasswordFromInitialRefreshTask;
		}
	}

	/// <summary>
	/// Creates a new <see cref="MySqlConnection"/> that can connect to the database represented by this <see cref="MySqlDataSource"/>.
	/// </summary>
	/// <remarks>
	/// <para>The connection must be opened before it can be used.</para>
	/// <para>It is the responsibility of the caller to properly dispose the connection returned by this method. Failure to do so may result in a connection leak.</para>
	/// </remarks>
	public new MySqlConnection CreateConnection() => (MySqlConnection) base.CreateConnection();

	/// <summary>
	/// Returns a new, open <see cref="MySqlConnection"/> to the database represented by this <see cref="MySqlDataSource"/>.
	/// </summary>
	/// <remarks>
	/// <para>The returned connection is already open, and is ready for immediate use.</para>
	/// <para>It is the responsibility of the caller to properly dispose the connection returned by this method. Failure to do so may result in a connection leak.</para>
	/// </remarks>
	public new MySqlConnection OpenConnection() => (MySqlConnection) base.OpenConnection();

	/// <summary>
	/// Asynchronously returns a new, open <see cref="MySqlConnection"/> to the database represented by this <see cref="MySqlDataSource"/>.
	/// </summary>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <remarks>
	/// <para>The returned connection is already open, and is ready for immediate use.</para>
	/// <para>It is the responsibility of the caller to properly dispose the connection returned by this method. Failure to do so may result in a connection leak.</para>
	/// </remarks>
	public new async ValueTask<MySqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default) =>
		(MySqlConnection) await base.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

	/// <summary>
	/// Gets the connection string of the database represented by this <see cref="MySqlDataSource"/>.
	/// </summary>
	public override string ConnectionString => m_connectionString;

#pragma warning disable CA1044 // Properties should not be write only
	/// <summary>
	/// Sets the password that will be used by the next <see cref="MySqlConnection"/> created from this <see cref="MySqlDataSource"/>.
	/// </summary>
	/// <remarks>
	/// <para>This can be used to update the password for database servers that periodically rotate authentication tokens, without
	/// affecting connection pooling. The <see cref="MySqlConnectionStringBuilder.Password"/> property must not be specified in
	/// order for this field to be used.</para>
	/// <para>Consider using <see cref="MySqlDataSourceBuilder.UsePeriodicPasswordProvider"/> instead.</para>
	/// </remarks>
	public string Password
	{
		set
		{
			if (m_periodicPasswordProvider is not null)
				throw new InvalidOperationException("Cannot set Password when this MySqlDataSource is configured with a PeriodicPasswordProvider.");

			m_password = value ?? throw new ArgumentNullException(nameof(value));
			m_providePasswordCallback = ProvidePasswordFromField;
		}
	}
#pragma warning restore CA1044 // Properties should not be write only

	protected override DbConnection CreateDbConnection()
	{
		if (m_isDisposed)
			throw new ObjectDisposedException(nameof(MySqlDataSource));
		return new MySqlConnection(this)
		{
			ProvideClientCertificatesCallback = m_clientCertificatesCallback,
			ProvidePasswordCallback = m_providePasswordCallback,
			RemoteCertificateValidationCallback = m_remoteCertificateValidationCallback,
		};
	}

	protected override void Dispose(bool disposing)
	{
		try
		{
#pragma warning disable CA2012 // Safe because method completes synchronously
			if (disposing)
				DisposeAsync(IOBehavior.Synchronous).GetAwaiter().GetResult();
#pragma warning restore CA2012
		}
		finally
		{
			base.Dispose(disposing);
		}
	}

	protected override ValueTask DisposeAsyncCore() => DisposeAsync(IOBehavior.Asynchronous);

	private async ValueTask DisposeAsync(IOBehavior ioBehavior)
	{
		if (m_passwordProviderTimerCancellationTokenSource is { } cts)
		{
			cts.Cancel();
			cts.Dispose();
		}
		if (Pool is not null)
		{
			await Pool.ClearAsync(ioBehavior, default).ConfigureAwait(false);
			Pool.Dispose();
		}
		m_isDisposed = true;
	}

	private async Task RefreshPassword()
	{
		try
		{
			// set the password from the callback, then queue another refresh after the 'success' interval
			m_password = await m_periodicPasswordProvider!(m_providePasswordContext!, m_passwordProviderTimerCancellationTokenSource!.Token).ConfigureAwait(false);
			m_providePasswordCallback = ProvidePasswordFromField;
			m_passwordProviderTimer!.Change(m_periodicPasswordProviderSuccessRefreshInterval, Timeout.InfiniteTimeSpan);
		}
		catch (Exception e)
		{
			// queue a refresh after the 'failure' interval
			Log.PeriodicPasswordProviderFailed(m_logger, e, m_id, e.Message);
			m_passwordProviderTimer!.Change(m_periodicPasswordProviderFailureRefreshInterval, Timeout.InfiniteTimeSpan);
			throw new MySqlException("The periodic password provider failed", e);
		}
	}

	internal ConnectionPool? Pool { get; }

	internal MySqlConnectorLoggingConfiguration LoggingConfiguration { get; }

	internal string? Name { get; }

	private string ProvidePasswordFromField(MySqlProvidePasswordContext context) => m_password!;

	private string ProvidePasswordFromInitialRefreshTask(MySqlProvidePasswordContext context)
	{
		if (m_password is null)
		{
			// password hasn't been set up, so wait (synchronously) for the task to complete the first time
			m_initialPasswordRefreshTask!.GetAwaiter().GetResult();
			m_providePasswordCallback = ProvidePasswordFromField;
		}
		return m_password!;
	}

	private static int s_lastId;

	private readonly ILogger m_logger;
	private readonly int m_id;
	private readonly string m_connectionString;
	private readonly Func<X509CertificateCollection, ValueTask>? m_clientCertificatesCallback;
	private readonly RemoteCertificateValidationCallback? m_remoteCertificateValidationCallback;
	private readonly Func<MySqlProvidePasswordContext, CancellationToken, ValueTask<string>>? m_periodicPasswordProvider;
	private readonly TimeSpan m_periodicPasswordProviderSuccessRefreshInterval;
	private readonly TimeSpan m_periodicPasswordProviderFailureRefreshInterval;
	private readonly MySqlProvidePasswordContext? m_providePasswordContext;
	private readonly CancellationTokenSource? m_passwordProviderTimerCancellationTokenSource;
	private readonly Timer? m_passwordProviderTimer;
	private readonly Task? m_initialPasswordRefreshTask;
	private bool m_isDisposed;
	private string? m_password;
	private Func<MySqlProvidePasswordContext, string>? m_providePasswordCallback;
}
