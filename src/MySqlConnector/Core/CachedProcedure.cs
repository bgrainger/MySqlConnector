using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Logging;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Core
{
	internal sealed class CachedProcedure
	{
		public static async Task<CachedProcedure> FillAsync(IOBehavior ioBehavior, MySqlConnection connection, string schema, string component, CancellationToken cancellationToken)
		{
			// try to use mysql.proc first, as it is much faster
			if (connection.Session.ServerVersion.Version < ServerVersions.RemovesMySqlProcTable && !connection.Session.ProcAccessDenied)
			{
				try
				{
					using (var cmd = connection.CreateCommand())
					{
						cmd.Transaction = connection.CurrentTransaction;
						cmd.CommandText = @"SELECT param_list, returns FROM mysql.proc WHERE db = @schema AND name = @component";
						cmd.Parameters.AddWithValue("@schema", schema);
						cmd.Parameters.AddWithValue("@component", component);
						using (var reader = (MySqlDataReader) await cmd.ExecuteReaderAsync(CommandBehavior.Default, ioBehavior, cancellationToken).ConfigureAwait(false))
						{
							var exists = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
							if (!exists)
								return null;

							var parametersSqlBytes = (byte[]) reader.GetValue(0);
							var returnsSqlBytes = (byte[]) reader.GetValue(1);

							// ASSUME this is UTF-8 encoded; it's possible that the `character_set_client` column would need to be used?
							var parametersSql = Encoding.UTF8.GetString(parametersSqlBytes);
							var returnsSql = Encoding.UTF8.GetString(returnsSqlBytes);

							var parsedParameters = ParseParameters(parametersSql);
							if (returnsSql.Length != 0)
							{
								var returnDataType = ParseDataType(returnsSql, out var unsigned, out var length);
								parsedParameters.Insert(0, CreateCachedParameter(0, null, null, returnDataType, unsigned, length, returnsSql));
							}

							return new CachedProcedure(schema, component, parsedParameters);
						}
					}
				}
				catch (MySqlException ex)
				{
					Log.Warn("Session{0} failed to retrieve metadata for Schema={1} Component={2}; falling back to INFORMATION_SCHEMA. Error: {3}", connection.Session.Id, schema, component, ex.Message);
					if ((MySqlErrorCode) ex.Number == MySqlErrorCode.TableAccessDenied)
						connection.Session.ProcAccessDenied = true;
				}
			}

			if (connection.Session.ServerVersion.Version < ServerVersions.SupportsProcedureCache)
			{
				Log.Warn("Session{0} ServerVersion={1} does not support cached procedures", connection.Session.Id, connection.Session.ServerVersion.OriginalString);
					return null;
			}

			var parameters = new List<CachedParameter>();
			int routineCount;
			using (var cmd = connection.CreateCommand())
			{
				cmd.Transaction = connection.CurrentTransaction;
				cmd.CommandText = @"SELECT COUNT(*)
					FROM information_schema.routines
					WHERE ROUTINE_SCHEMA = @schema AND ROUTINE_NAME = @component;
					SELECT ORDINAL_POSITION, PARAMETER_MODE, PARAMETER_NAME, DTD_IDENTIFIER
					FROM information_schema.parameters
					WHERE SPECIFIC_SCHEMA = @schema AND SPECIFIC_NAME = @component
					ORDER BY ORDINAL_POSITION";
				cmd.Parameters.AddWithValue("@schema", schema);
				cmd.Parameters.AddWithValue("@component", component);

				using (var reader = (MySqlDataReader) await cmd.ExecuteReaderAsync(CommandBehavior.Default, ioBehavior, cancellationToken).ConfigureAwait(false))
				{
					await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
					routineCount = reader.GetInt32(0);
					await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);

					while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
					{
						var dataType = ParseDataType(reader.GetString(3), out var unsigned, out var length);
						parameters.Add(new CachedParameter(
							reader.GetInt32(0),
							!reader.IsDBNull(1) ? reader.GetString(1) : null,
							!reader.IsDBNull(2) ? reader.GetString(2) : null,
							dataType,
							unsigned,
							length
						));
					}
				}
			}

			if (Log.IsInfoEnabled())
				Log.Info("Procedure for Schema={0} Component={1} has RoutineCount={2}, ParameterCount={3}", schema, component, routineCount, parameters.Count);
			return routineCount == 0 ? null : new CachedProcedure(schema, component, parameters);
		}

		public IReadOnlyList<CachedParameter> Parameters { get; }

		private CachedProcedure(string schema, string component, IReadOnlyList<CachedParameter> parameters)
		{
			m_schema = schema;
			m_component = component;
			Parameters = parameters;
		}

		internal MySqlParameterCollection AlignParamsWithDb(MySqlParameterCollection parameterCollection)
		{
			var alignedParams = new MySqlParameterCollection();
			var returnParam = parameterCollection?.FirstOrDefault(x => x.Direction == ParameterDirection.ReturnValue);

			foreach (var cachedParam in Parameters)
			{
				MySqlParameter alignParam;
				if (cachedParam.Direction == ParameterDirection.ReturnValue)
				{
					alignParam = returnParam ?? throw new InvalidOperationException($"Attempt to call stored function {FullyQualified} without specifying a return parameter");
				}
				else
				{
					var index = parameterCollection?.NormalizedIndexOf(cachedParam.Name) ?? -1;
					alignParam = index >= 0 ? parameterCollection[index] : throw new ArgumentException($"Parameter '{cachedParam.Name}' not found in the collection.");
				}

				if (!alignParam.HasSetDirection)
					alignParam.Direction = cachedParam.Direction;
				if (!alignParam.HasSetDbType)
					alignParam.MySqlDbType = cachedParam.MySqlDbType;

				// cached parameters are oredered by ordinal position
				alignedParams.Add(alignParam);
			}

			return alignedParams;
		}

		internal static List<CachedParameter> ParseParameters(string parametersSql)
		{
			// strip comments
			parametersSql = Regex.Replace(parametersSql, @"/\*.*?\*/", "", RegexOptions.Singleline);
			parametersSql = Regex.Replace(parametersSql, @"(^|\s)--.*?$", "", RegexOptions.Multiline);

			// normalize spaces
			parametersSql = Regex.Replace(parametersSql, @"\s+", " ");

			if (string.IsNullOrWhiteSpace(parametersSql))
				return new List<CachedParameter>();

			// strip precision specifier containing comma
			parametersSql = Regex.Replace(parametersSql, @"(DECIMAL|DEC|FIXED|NUMERIC|FLOAT|DOUBLE PRECISION|DOUBLE|REAL)\s*\(\d+(,\s*\d+)\)", @"$1", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

			var parameters = parametersSql.Split(',');
			var cachedParameters = new List<CachedParameter>(parameters.Length);
			for (var i = 0; i < parameters.Length; i++)
			{
				var parameter = parameters[i].Trim();
				var originalString = parameter;
				string direction = "IN";
				if (parameter.StartsWith("INOUT ", StringComparison.OrdinalIgnoreCase))
				{
					direction = "INOUT";
					parameter = parameter.Substring(6);
				}
				else if (parameter.StartsWith("OUT ", StringComparison.OrdinalIgnoreCase))
				{
					direction = "OUT";
					parameter = parameter.Substring(4);
				}
				else if (parameter.StartsWith("IN ", StringComparison.OrdinalIgnoreCase))
				{
					direction = "IN";
					parameter = parameter.Substring(3);
				}

				var parts = Regex.Match(parameter, @"^(?:`((?:[\u0001-\u005F\u0061-\uFFFF]+|``)+)`|([A-Za-z0-9$_\u0080-\uFFFF]+)) (.*)$");
				var name = parts.Groups[1].Success ? parts.Groups[1].Value.Replace("``", "`") : parts.Groups[2].Value;

				var dataType = ParseDataType(parts.Groups[3].Value, out var unsigned, out var length);
				cachedParameters.Add(CreateCachedParameter(i + 1, direction, name, dataType, unsigned, length, originalString));
			}

			return cachedParameters;
		}

		internal static string ParseDataType(string sql, out bool unsigned, out int length)
		{
			if (sql.EndsWith(" ZEROFILL", StringComparison.OrdinalIgnoreCase))
				sql = sql.Substring(0, sql.Length - 9);
			unsigned = false;
			if (sql.EndsWith(" UNSIGNED", StringComparison.OrdinalIgnoreCase))
			{
				unsigned = true;
				sql = sql.Substring(0, sql.Length - 9);
			}
			sql = Regex.Replace(sql, " (CHARSET|CHARACTER SET) [A-Za-z0-9_]+", "", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);
			sql = Regex.Replace(sql, " (COLLATE) [A-Za-z0-9_]+", "", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

			length = 0;
			var match = Regex.Match(sql, @"\s*\(\s*(\d+)\s*(?:,\s*\d+\s*)?\)");
			if (match.Success)
			{
				length = int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
				sql = Regex.Replace(sql, @"\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\)", "");
			}

			sql = sql.Trim();

			// normalize alternate data type names
			if (sql.Equals("BOOL", StringComparison.OrdinalIgnoreCase) || sql.Equals("BOOLEAN", StringComparison.OrdinalIgnoreCase))
			{
				sql = "TINYINT";
				length = 1;
			}
			else if (sql.Equals("INTEGER", StringComparison.OrdinalIgnoreCase))
			{
				sql = "INT";
			}
			else if (sql.Equals("NUMERIC", StringComparison.OrdinalIgnoreCase) || sql.Equals("FIXED", StringComparison.OrdinalIgnoreCase))
			{
				sql = "DECIMAL";
			}
			else if (sql.Equals("REAL", StringComparison.OrdinalIgnoreCase) || sql.Equals("DOUBLE PRECISION", StringComparison.OrdinalIgnoreCase))
			{
				sql = "DOUBLE";
			}
			else if (sql.Equals("NVARCHAR", StringComparison.OrdinalIgnoreCase) || sql.Equals("CHARACTER VARYING", StringComparison.OrdinalIgnoreCase) || sql.Equals("NATIONAL VARCHAR", StringComparison.OrdinalIgnoreCase))
			{
				sql = "VARCHAR";
			}
			else if (sql.Equals("NCHAR", StringComparison.OrdinalIgnoreCase) || sql.Equals("CHARACTER", StringComparison.OrdinalIgnoreCase) || sql.Equals("NATIONAL CHAR", StringComparison.OrdinalIgnoreCase))
			{
				sql = "CHAR";
			}
			else if (sql.Equals("CHAR BYTE", StringComparison.OrdinalIgnoreCase))
			{
				sql = "BINARY";
			}

			return sql;
		}

		private static CachedParameter CreateCachedParameter(int ordinal, string direction, string name, string dataType, bool unsigned, int length, string originalSql)
		{
			try
			{
				return new CachedParameter(ordinal, direction, name, dataType, unsigned, length);
			}
			catch (NullReferenceException ex)
			{
				throw new MySqlException("Failed to parse stored procedure parameter '{0}'; extracted data type was {1}".FormatInvariant(originalSql, dataType), ex);
			}
		}

		string FullyQualified => $"`{m_schema}`.`{m_component}`";

		static readonly IMySqlConnectorLogger Log = MySqlConnectorLogManager.CreateLogger(nameof(CachedProcedure));

		readonly string m_schema;
		readonly string m_component;
	}
}
