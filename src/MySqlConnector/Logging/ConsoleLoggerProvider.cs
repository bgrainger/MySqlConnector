using System;
using System.Globalization;
using System.Text;

namespace MySqlConnector.Logging
{
	public class ConsoleLoggerProvider : IMySqlConnectorLoggerProvider
	{
		public ConsoleLoggerProvider(MySqlConnectorLogLevel minimumLevel = MySqlConnectorLogLevel.Info, bool isColored = true)
		{
			if (minimumLevel < MySqlConnectorLogLevel.Trace || minimumLevel > MySqlConnectorLogLevel.Fatal)
				throw new ArgumentOutOfRangeException(nameof(minimumLevel), "minimumLevel must be between Trace and Fatal");

			m_minimumLevel = minimumLevel;
			m_isColored = isColored;
		}

		public IMySqlConnectorLogger CreateLogger(string name) => new ConsoleLogger(this, name);

		private sealed class ConsoleLogger : IMySqlConnectorLogger
		{
			public ConsoleLogger(ConsoleLoggerProvider provider, string name)
			{
				m_provider = provider;
				m_name = name;
			}

			public bool IsEnabled(MySqlConnectorLogLevel level) => level >= m_provider.m_minimumLevel && level <= MySqlConnectorLogLevel.Fatal;

			public void Log(MySqlConnectorLogLevel level, string message, object?[]? args = null, Exception? exception = null)
			{
				if (!IsEnabled(level))
					return;

				var sb = new StringBuilder();
				sb.Append(s_levels[(int) level]);
				sb.Append('\t');
				sb.Append(m_name);
				sb.Append('\t');

				if (args == null || args.Length == 0)
					sb.Append(message);
				else
					sb.AppendFormat(CultureInfo.InvariantCulture, message, args);
				sb.AppendLine();

				if (exception != null)
					sb.AppendLine(exception.ToString());

				if (m_provider.m_isColored)
				{
					lock (m_provider)
					{
						var oldColor = Console.ForegroundColor;
						Console.ForegroundColor = s_colors[(int) level];
						Console.Error.Write(sb.ToString());
						Console.ForegroundColor = oldColor;
					}
				}
				else
				{
					Console.Error.Write(sb.ToString());
				}
			}

			static readonly string[] s_levels =
			{
				"",
				"[TRACE]",
				"[DEBUG]",
				"[INFO]",
				"[WARN]",
				"[ERROR]",
				"[FATAL]",
			};

			static readonly ConsoleColor[] s_colors =
			{
				ConsoleColor.Black,
				ConsoleColor.DarkGray,
				ConsoleColor.Gray,
				ConsoleColor.White,
				ConsoleColor.Yellow,
				ConsoleColor.Red,
				ConsoleColor.Red,
			};

			readonly ConsoleLoggerProvider m_provider;
			readonly string m_name;
		}

		readonly MySqlConnectorLogLevel m_minimumLevel;
		readonly bool m_isColored;
	}
}
