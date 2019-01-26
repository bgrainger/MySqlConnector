using System;
using System.Globalization;
using NLog;

namespace MySqlConnector.Logging
{
	public sealed class NLogLoggerProvider : IMySqlConnectorLoggerProvider
	{
		public IMySqlConnectorLogger CreateLogger(string name) => new NLogLogger(LogManager.GetLogger("MySqlConnector." + name));

		static readonly Type s_loggerType = typeof(NLogLogger);

		private class NLogLogger : IMySqlConnectorLogger
		{
			public NLogLogger(Logger logger) => m_logger = logger;

			public bool IsEnabled(MySqlConnectorLogLevel level) => m_logger.IsEnabled(GetLevel(level));

			public void Log(MySqlConnectorLogLevel level, string message, object[] args = null, Exception exception = null)
			{
				LogLevel logLevel = GetLevel(level);
				if (m_logger.IsEnabled(logLevel))
				{
					m_logger.Log(s_loggerType, LogEventInfo.Create(logLevel, m_logger.Name, exception, CultureInfo.InvariantCulture, message, args));
				}
			}

			private static LogLevel GetLevel(MySqlConnectorLogLevel level) => level switch
			{
				MySqlConnectorLogLevel.Trace => LogLevel.Trace,
				MySqlConnectorLogLevel.Debug => LogLevel.Debug,
				MySqlConnectorLogLevel.Info => LogLevel.Info,
				MySqlConnectorLogLevel.Warn => LogLevel.Warn,
				MySqlConnectorLogLevel.Error => LogLevel.Error,
				MySqlConnectorLogLevel.Fatal => LogLevel.Fatal,
				_ => throw new ArgumentOutOfRangeException(nameof(level), level, "Invalid value for 'level'.")
			};

		readonly Logger m_logger;
		}
	}
}
