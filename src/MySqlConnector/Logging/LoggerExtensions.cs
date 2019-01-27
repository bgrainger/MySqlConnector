using System;

namespace MySqlConnector.Logging
{
	internal static class LoggerExtensions
	{
		public static bool IsTraceEnabled(this IMySqlConnectorLogger log) => log.IsEnabled(MySqlConnectorLogLevel.Trace);
		public static bool IsDebugEnabled(this IMySqlConnectorLogger log) => log.IsEnabled(MySqlConnectorLogLevel.Debug);
		public static bool IsInfoEnabled(this IMySqlConnectorLogger log) => log.IsEnabled(MySqlConnectorLogLevel.Info);
		public static bool IsWarnEnabled(this IMySqlConnectorLogger log) => log.IsEnabled(MySqlConnectorLogLevel.Warn);
		public static bool IsErrorEnabled(this IMySqlConnectorLogger log) => log.IsEnabled(MySqlConnectorLogLevel.Error);
		public static bool IsFatalEnabled(this IMySqlConnectorLogger log) => log.IsEnabled(MySqlConnectorLogLevel.Fatal);

		public static void Trace(this IMySqlConnectorLogger log, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Trace, message, args, null);
		public static void Debug(this IMySqlConnectorLogger log, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Debug, message, args, null);
		public static void Info(this IMySqlConnectorLogger log, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Info, message, args, null);
		public static void Warn(this IMySqlConnectorLogger log, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Warn, message, args, null);
		public static void Error(this IMySqlConnectorLogger log, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Error, message, args, null);
		public static void Fatal(this IMySqlConnectorLogger log, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Fatal, message, args, null);

		public static void Trace(this IMySqlConnectorLogger log, Exception exception, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Trace, message, args, exception);
		public static void Debug(this IMySqlConnectorLogger log, Exception exception, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Debug, message, args, exception);
		public static void Info(this IMySqlConnectorLogger log, Exception exception, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Info, message, args, exception);
		public static void Warn(this IMySqlConnectorLogger log, Exception exception, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Warn, message, args, exception);
		public static void Error(this IMySqlConnectorLogger log, Exception exception, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Error, message, args, exception);
		public static void Fatal(this IMySqlConnectorLogger log, Exception exception, string message, params object?[] args) => log.Log(MySqlConnectorLogLevel.Fatal, message, args, exception);
	}
}
