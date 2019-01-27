using System;
using System.Collections.Generic;

namespace MySqlConnector.Core
{
	/// <summary>
	/// <see cref="ParsedStatement"/> represents an individual SQL statement that's been parsed
	/// from a string possibly containing multiple semicolon-delimited SQL statements.
	/// </summary>
	internal sealed class ParsedStatement
	{
		/// <summary>
		/// The bytes for this statement that will be written on the wire.
		/// </summary>
		public ArraySegment<byte> StatementBytes { get; set; }

		/// <summary>
		/// The names of the parameters (if known) of the parameters in the prepared statement. There
		/// is one entry in this list for each parameter, which will be <c>null</c> if the name is unknown.
		/// </summary>
		public List<string?> ParameterNames { get; } = new List<string?>();

		/// <summary>
		/// The indexes of the parameters in the prepared statement. There is one entry in this list for
		/// each parameter; it will be <c>-1</c> if the parameter is named.
		/// </summary>
		public List<int> ParameterIndexes { get; }= new List<int>();
	}
}
