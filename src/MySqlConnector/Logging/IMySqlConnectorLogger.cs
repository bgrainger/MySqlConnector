using System;

namespace MySqlConnector.Logging
{
	/// <summary>
	/// Implementations of <see cref="IMySqlConnectorLogger"/> write logs to a particular target.
	/// </summary>
	public interface IMySqlConnectorLogger
	{
		/// <summary>
		/// Returns <c>true</c> if logging for this logger is enabled at the specified level.
		/// </summary>
		/// <param name="level">The log level.</param>
		/// <returns><c>true</c> if logging is enabled; otherwise, <c>false</c>.</returns>
		bool IsEnabled(MySqlConnectorLogLevel level);

		/// <summary>
		/// Writes a log message to the target.
		/// </summary>
		/// <param name="level">The log level.</param>
		/// <param name="message">The log message. See documentation for <paramref name="args"/> for notes on interpreting <c>{0}</c> within this string.</param>
		/// <param name="args">If not <c>null</c> or empty, then <paramref name="message"/> includes formatting placeholders (e.g., <c>{0}</c>)
		/// which must be replaced with the arguments in <paramref name="args"/>, using <see cref="string.Format(System.IFormatProvider,string,object[])"/> or similar.
		/// If <c>null</c> or an empty array, then <paramref name="message"/> is a literal string; any curly braces within it must be treated as literal characters,
		/// not formatting placeholders.</param>
		/// <param name="exception">If not <c>null</c>, an <see cref="Exception"/> associated with the log message.</param>
		/// <remarks>This method may be called from multiple threads and must be thread-safe. This method may be called
		/// even if <see cref="IsEnabled"/> would return <c>false</c> for <paramref name="level"/>; the implementation must
		/// check if logging is enabled for that level.</remarks>
		void Log(MySqlConnectorLogLevel level, string message, object?[]? args = null, Exception? exception = null);
	}
}
