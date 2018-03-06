using System;
using System.Data;
using MySql.Data.MySqlClient;
using MySqlConnector.Protocol;
using MySqlConnector.Utilities;

namespace MySqlConnector.Core
{
	internal sealed class StatementPreparer
	{
		public StatementPreparer(StatementWriter writer, string commandText, MySqlParameterCollection parameters, StatementPreparerOptions options)
		{
			m_writer = writer;
			m_commandText = commandText;
			m_parameters = parameters;
			m_options = options;
		}

		public ArraySegment<byte> ParseAndBindParameters()
		{
			m_writer.Write((byte) CommandKind.Query);

			if (!string.IsNullOrWhiteSpace(m_commandText))
			{
				var parser = new ParameterSqlParser(this, m_writer);
				parser.Parse(m_commandText);
			}

			return m_writer.RawBuffer;
		}

		private sealed class ParameterSqlParser : SqlParser
		{
			public ParameterSqlParser(StatementPreparer preparer, StatementWriter writer)
			{
				m_preparer = preparer;
				m_writer = writer;
			}

			protected override void OnBeforeParse(string sql)
			{
			}

			protected override void OnNamedParameter(int index, int length)
			{
				var parameterName = m_preparer.m_commandText.Substring(index, length);
				var parameterIndex = m_preparer.m_parameters.NormalizedIndexOf(parameterName);
				if (parameterIndex != -1)
					DoAppendParameter(parameterIndex, index, length);
				else if ((m_preparer.m_options & StatementPreparerOptions.AllowUserVariables) == 0)
					throw new MySqlException("Parameter '{0}' must be defined. To use this as a variable, set 'Allow User Variables=true' in the connection string.".FormatInvariant(parameterName));
			}

			protected override void OnPositionalParameter(int index)
			{
				DoAppendParameter(m_currentParameterIndex, index, 1);
				m_currentParameterIndex++;
			}

			private void DoAppendParameter(int parameterIndex, int textIndex, int textLength)
			{
				AppendString(m_preparer.m_commandText, m_lastIndex, textIndex - m_lastIndex);
				var parameter = m_preparer.m_parameters[parameterIndex];
				if (parameter.Direction != ParameterDirection.Input && (m_preparer.m_options & StatementPreparerOptions.AllowOutputParameters) == 0)
					throw new MySqlException("Only ParameterDirection.Input is supported when CommandType is Text (parameter name: {0})".FormatInvariant(parameter.ParameterName));
				m_preparer.m_parameters[parameterIndex].AppendSqlString(m_writer, m_preparer.m_options);
				m_lastIndex = textIndex + textLength;
			}

			protected override void OnParsed()
			{
				AppendString(m_preparer.m_commandText, m_lastIndex, m_preparer.m_commandText.Length - m_lastIndex);
			}

			private void AppendString(string value, int offset, int length)
			{
				m_writer.WriteUtf8(value, offset, length);
			}

			readonly StatementPreparer m_preparer;
			readonly StatementWriter m_writer;
			int m_currentParameterIndex;
			int m_lastIndex;
		}

		readonly StatementWriter m_writer;
		readonly string m_commandText;
		readonly MySqlParameterCollection m_parameters;
		readonly StatementPreparerOptions m_options;
	}
}
