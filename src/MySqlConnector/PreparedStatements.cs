using System;
using System.Collections.Generic;

namespace MySqlConnector.Core
{
	internal sealed class PreparedStatements : IDisposable
	{
		public IReadOnlyList<PreparedStatement> Statements { get; }

		public PreparedStatements(IReadOnlyList<PreparedStatement> preparedStatements, ParsedStatements parsedStatements)
		{
			Statements = preparedStatements;
			m_parsedStatements = parsedStatements;
		}

		public void Dispose()
		{
			m_parsedStatements?.Dispose();
			m_parsedStatements = null;
		}

		ParsedStatements? m_parsedStatements;
	}
}
