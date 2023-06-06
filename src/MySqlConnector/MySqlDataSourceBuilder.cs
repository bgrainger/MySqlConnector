using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using MySqlConnector.Logging;

namespace MySqlConnector;

/// <summary>
/// <see cref="MySqlDataSourceBuilder"/> provides an API for configuring and creating a <see cref="MySqlDataSource"/>,
/// from which <see cref="MySqlConnection"/> objects can be obtained.
/// </summary>
public sealed class MySqlDataSourceBuilder
{
	/// <summary>
	/// Initializes a new <see cref="MySqlDataSourceBuilder"/> with the specified connection string.
	/// </summary>
	/// <param name="connectionString">The optional connection string to use.</param>
	public MySqlDataSourceBuilder(string? connectionString = null)
	{
		ConnectionStringBuilder = new(connectionString ?? "");
	}

	/// <summary>
	/// Sets the <see cref="ILoggerFactory"/> that will be used for logging.
	/// </summary>
	/// <param name="loggerFactory">The logger factory.</param>
	/// <returns>This builder, so that method calls can be chained.</returns>
	public MySqlDataSourceBuilder UseLoggerFactory(ILoggerFactory? loggerFactory)
	{
		m_loggerFactory = loggerFactory;
		return this;
	}

	/// <summary>
	/// Sets the name of the <see cref="MySqlDataSource"/> that will be created.
	/// </summary>
	/// <param name="name">The data source name.</param>
	/// <returns>This builder, so that method calls can be chained.</returns>
	/// <remarks>The name will be used to identify metrics for the <see cref="MySqlDataSource"/>.</remarks>
	public MySqlDataSourceBuilder UseName(string? name)
	{
		m_name = name;
		return this;
	}

	/// <summary>
	/// Sets the callback used to provide client certificates for connecting to a server.
	/// </summary>
	/// <param name="callback">The callback that will provide client certificates. The <see cref="X509CertificateCollection"/>
	/// provided to the callback should be filled with the client certificate(s) needed to connect to the server.</param>
	/// <returns>This builder, so that method calls can be chained.</returns>
	public MySqlDataSourceBuilder UseClientCertificatesCallback(Func<X509CertificateCollection, ValueTask>? callback)
	{
		m_clientCertificatesCallback = callback;
		return this;
	}

	/// <summary>
	/// Configures a periodic password provider, which is automatically called by the data source at some regular interval. This is the
	/// recommended way to fetch a rotating access token.
	/// </summary>
	/// <param name="passwordProvider">A callback which returns the password to be used by any new MySQL connections that are made.</param>
	/// <param name="successRefreshInterval">How long to cache the password before re-invoking the callback.</param>
	/// <param name="failureRefreshInterval">How long to wait before re-invoking the callback on failure. This should
	/// typically be much shorter than <paramref name="successRefreshInterval"/>.</param>
	/// <returns>This builder, so that method calls can be chained.</returns>
	public MySqlDataSourceBuilder UsePeriodicPasswordProvider(Func<MySqlProvidePasswordContext, CancellationToken, ValueTask<string>>? passwordProvider, TimeSpan successRefreshInterval, TimeSpan failureRefreshInterval)
	{
		m_periodicPasswordProvider = passwordProvider;
		m_periodicPasswordProviderSuccessRefreshInterval = successRefreshInterval;
		m_periodicPasswordProviderFailureRefreshInterval = failureRefreshInterval;
		return this;
	}

	/// <summary>
	/// Sets the callback used to verify that the server's certificate is valid.
	/// </summary>
	/// <param name="callback">The callback used to verify that the server's certificate is valid.</param>
	/// <returns>This builder, so that method calls can be chained.</returns>
	/// <remarks><see cref="MySqlConnectionStringBuilder.SslMode"/> must be set to <see cref="MySqlSslMode.Preferred"/>
	/// or <see cref="MySqlSslMode.Required"/> in order for this delegate to be invoked. See the documentation for
	/// <see cref="RemoteCertificateValidationCallback"/> for more information on the values passed to this delegate.</remarks>
	public MySqlDataSourceBuilder UseRemoteCertificateValidationCallback(RemoteCertificateValidationCallback callback)
	{
		m_remoteCertificateValidationCallback = callback;
		return this;
	}

	/// <summary>
	/// Builds a <see cref="MySqlDataSource"/> which is ready for use.
	/// </summary>
	/// <returns>A new <see cref="MySqlDataSource"/> with the settings configured through this <see cref="MySqlDataSourceBuilder"/>.</returns>
	public MySqlDataSource Build()
	{
		var loggingConfiguration = m_loggerFactory is null ? MySqlConnectorLoggingConfiguration.NullConfiguration : new(m_loggerFactory);
		return new(ConnectionStringBuilder.ConnectionString,
			loggingConfiguration,
			m_name,
			m_clientCertificatesCallback,
			m_remoteCertificateValidationCallback,
			m_periodicPasswordProvider,
			m_periodicPasswordProviderSuccessRefreshInterval,
			m_periodicPasswordProviderFailureRefreshInterval
			);
	}

	/// <summary>
	/// A <see cref="MySqlConnectionStringBuilder"/> that can be used to configure the connection string on this <see cref="MySqlDataSourceBuilder"/>.
	/// </summary>
	public MySqlConnectionStringBuilder ConnectionStringBuilder { get; }

	private ILoggerFactory? m_loggerFactory;
	private string? m_name;
	private Func<X509CertificateCollection, ValueTask>? m_clientCertificatesCallback;
	private RemoteCertificateValidationCallback? m_remoteCertificateValidationCallback;
	private Func<MySqlProvidePasswordContext, CancellationToken, ValueTask<string>>? m_periodicPasswordProvider;
	private TimeSpan m_periodicPasswordProviderSuccessRefreshInterval;
	private TimeSpan m_periodicPasswordProviderFailureRefreshInterval;
}
