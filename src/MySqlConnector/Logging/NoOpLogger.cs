using System;

namespace MySqlConnector.Logging
{
	/// <summary>
	/// <see cref="NoOpLogger"/> is an implementation of <see cref="IMySqlConnectorLogger"/> that does nothing.
	/// </summary>
	/// <remarks>This is the default logging implementation unless <see cref="MySqlConnectorLogManager.Provider"/> is set.</remarks>
	public sealed class NoOpLogger : IMySqlConnectorLogger
	{
		/// <summary>
		/// Returns <c>false</c>.
		/// </summary>
		public bool IsEnabled(MySqlConnectorLogLevel level) => false;

		/// <summary>
		/// Ignores the specified log message.
		/// </summary>
		public void Log(MySqlConnectorLogLevel level, string message, object?[]? args = null, Exception? exception = null)
		{
		}

		/// <summary>
		/// Returns a singleton instance of <see cref="NoOpLogger"/>.
		/// </summary>
		public static IMySqlConnectorLogger Instance { get; } = new NoOpLogger();
	}
}
