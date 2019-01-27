using System;
using System.Collections.Generic;
using System.Data;
using MySql.Data.MySqlClient;
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Core
{
	internal sealed class StatementPreparer
	{
		public StatementPreparer(string commandText, MySqlParameterCollection? parameters, StatementPreparerOptions options)
		{
			m_commandText = commandText;
			m_parameters = parameters;
			m_options = options;
		}

		public ParsedStatements SplitStatements()
		{
			var statements = new List<ParsedStatement>();
			var statementStartEndIndexes = new List<int>();
			var writer = new ByteBufferWriter(m_commandText.Length + 1);
			var parser = new PreparedCommandSqlParser(this, statements, statementStartEndIndexes, writer);
			parser.Parse(m_commandText);
			for (var i = 0; i < statements.Count; i++)
				statements[i].StatementBytes = writer.ArraySegment.Slice(statementStartEndIndexes[i * 2], statementStartEndIndexes[i * 2 + 1] - statementStartEndIndexes[i * 2]);
			return new ParsedStatements(statements, writer.ToPayloadData());
		}

		public ArraySegment<byte> ParseAndBindParameters()
		{
			var writer = new ByteBufferWriter(m_commandText.Length + 1);
			writer.Write((byte) CommandKind.Query);

			if (!string.IsNullOrWhiteSpace(m_commandText))
			{
				var parser = new ParameterSqlParser(this, writer);
				parser.Parse(m_commandText);
			}

			return writer.ArraySegment;
		}

		private int GetParameterIndex(string name)
		{
			var index = m_parameters?.NormalizedIndexOf(name) ?? -1;
			if (index == -1 && (m_options & StatementPreparerOptions.AllowUserVariables) == 0)
				throw new MySqlException("Parameter '{0}' must be defined. To use this as a variable, set 'Allow User Variables=true' in the connection string.".FormatInvariant(name));
			return index;
		}

		private MySqlParameter GetInputParameter(int index)
		{
			if (index >= (m_parameters?.Count ?? 0))
				throw new MySqlException("Parameter index {0} is invalid when only {1} parameter{2} defined.".FormatInvariant(index, m_parameters?.Count ?? 0, m_parameters?.Count == 1 ? " is" : "s are"));
			var parameter = m_parameters![index];
			if (parameter.Direction != ParameterDirection.Input && (m_options & StatementPreparerOptions.AllowOutputParameters) == 0)
				throw new MySqlException("Only ParameterDirection.Input is supported when CommandType is Text (parameter name: {0})".FormatInvariant(parameter.ParameterName));
			return parameter;
		}

		private sealed class ParameterSqlParser : SqlParser
		{
			public ParameterSqlParser(StatementPreparer preparer, ByteBufferWriter writer)
			{
				m_preparer = preparer;
				m_writer = writer;
			}

			protected override void OnNamedParameter(int index, int length)
			{
				var parameterIndex = m_preparer.GetParameterIndex(m_preparer.m_commandText.Substring(index, length));
				if (parameterIndex != -1)
					DoAppendParameter(parameterIndex, index, length);
			}

			protected override void OnPositionalParameter(int index)
			{
				DoAppendParameter(m_currentParameterIndex, index, 1);
				m_currentParameterIndex++;
			}

			private void DoAppendParameter(int parameterIndex, int textIndex, int textLength)
			{
				m_writer.Write(m_preparer.m_commandText, m_lastIndex, textIndex - m_lastIndex);
				var parameter = m_preparer.GetInputParameter(parameterIndex);
				parameter.AppendSqlString(m_writer, m_preparer.m_options);
				m_lastIndex = textIndex + textLength;
			}

			protected override void OnParsed()
			{
				m_writer.Write(m_preparer.m_commandText, m_lastIndex, m_preparer.m_commandText.Length - m_lastIndex);
			}

			readonly StatementPreparer m_preparer;
			readonly ByteBufferWriter m_writer;
			int m_currentParameterIndex;
			int m_lastIndex;
		}

		private sealed class PreparedCommandSqlParser : SqlParser
		{
			public PreparedCommandSqlParser(StatementPreparer preparer, List<ParsedStatement> statements, List<int> statementStartEndIndexes, ByteBufferWriter writer)
			{
				m_preparer = preparer;
				m_statements = statements;
				m_statementStartEndIndexes = statementStartEndIndexes;
				m_writer = writer;
			}

			protected override void OnStatementBegin(int index)
			{
				m_statements.Add(new ParsedStatement());
				m_statementStartEndIndexes.Add(m_writer.Position);
				m_writer.Write((byte) CommandKind.StatementPrepare);
				m_lastIndex = index;
			}

			protected override void OnNamedParameter(int index, int length)
			{
				var parameterName = m_preparer.m_commandText.Substring(index, length);
				DoAppendParameter(parameterName, -1, index, length);
			}

			protected override void OnPositionalParameter(int index)
			{
				DoAppendParameter(null, m_currentParameterIndex, index, 1);
				m_currentParameterIndex++;
			}

			private void DoAppendParameter(string? parameterName, int parameterIndex, int textIndex, int textLength)
			{
				// write all SQL up to the parameter
				m_writer.Write(m_preparer.m_commandText, m_lastIndex, textIndex - m_lastIndex);
				m_lastIndex = textIndex + textLength;

				// replace the parameter with a ? placeholder
				m_writer.Write((byte) '?');

				// store the parameter index
				m_statements[m_statements.Count - 1].ParameterNames.Add(parameterName);
				m_statements[m_statements.Count - 1].ParameterIndexes.Add(parameterIndex);
			}

			protected override void OnStatementEnd(int index)
			{
				m_writer.Write(m_preparer.m_commandText, m_lastIndex, index - m_lastIndex);
				m_lastIndex = index;
				m_statementStartEndIndexes.Add(m_writer.Position);
			}

			readonly StatementPreparer m_preparer;
			readonly List<ParsedStatement> m_statements;
			readonly List<int> m_statementStartEndIndexes;
			readonly ByteBufferWriter m_writer;
			int m_currentParameterIndex;
			int m_lastIndex;
		}


		readonly string m_commandText;
		readonly MySqlParameterCollection? m_parameters;
		readonly StatementPreparerOptions m_options;
	}
}
