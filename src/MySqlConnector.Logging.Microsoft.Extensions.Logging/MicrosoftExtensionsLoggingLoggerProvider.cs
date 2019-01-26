using System;
using System.Globalization;
using Microsoft.Extensions.Logging;

namespace MySqlConnector.Logging
{
	public sealed class MicrosoftExtensionsLoggingLoggerProvider : IMySqlConnectorLoggerProvider
	{
		public MicrosoftExtensionsLoggingLoggerProvider(ILoggerFactory loggerFactory) => m_loggerFactory = loggerFactory;

		public IMySqlConnectorLogger CreateLogger(string name) => new MicrosoftExtensionsLoggingLogger(m_loggerFactory.CreateLogger(name));

		private class MicrosoftExtensionsLoggingLogger : IMySqlConnectorLogger
		{
			public MicrosoftExtensionsLoggingLogger(ILogger logger) => m_logger = logger;

			public bool IsEnabled(MySqlConnectorLogLevel level) => m_logger.IsEnabled(GetLevel(level));

			public void Log(MySqlConnectorLogLevel level, string message, object[] args = null, Exception exception = null)
			{
				if (args == null || args.Length == 0)
					m_logger.Log(GetLevel(level), 0, message, exception, s_getMessage);
				else
					m_logger.Log(GetLevel(level), 0, (message, args), exception, s_messageFormatter);
			}

			private static LogLevel GetLevel(MySqlConnectorLogLevel level) => level switch
			{
				MySqlConnectorLogLevel.Trace => LogLevel.Trace,
				MySqlConnectorLogLevel.Debug => LogLevel.Debug,
				MySqlConnectorLogLevel.Info => LogLevel.Information,
				MySqlConnectorLogLevel.Warn => LogLevel.Warning,
				MySqlConnectorLogLevel.Error => LogLevel.Error,
				MySqlConnectorLogLevel.Fatal => LogLevel.Critical,
				_ => throw new ArgumentOutOfRangeException(nameof(level), level, "Invalid value for 'level'.")
			};

			static readonly Func<string, Exception, string> s_getMessage = (s, e) => s;
			static readonly Func<(string Message, object[] Args), Exception, string> s_messageFormatter = (s, e) => string.Format(CultureInfo.InvariantCulture, s.Message, s.Args);

			readonly ILogger m_logger;
		}

		readonly ILoggerFactory m_loggerFactory;
	}
}
