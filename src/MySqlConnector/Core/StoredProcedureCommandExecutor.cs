using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Protocol.Serialization;

namespace MySqlConnector.Core
{
	internal sealed class StoredProcedureCommandExecutor : TextCommandExecutor
	{

		internal StoredProcedureCommandExecutor(MySqlCommand command)
			: base(command)
		{
			m_command = command;
			m_outParams = new MySqlParameterCollection();
			m_outParamNames = new List<string>();
		}

		public override async Task<DbDataReader> ExecuteReaderAsync(string commandText, MySqlParameterCollection? parameterCollection,
			CommandBehavior behavior, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			var cachedProcedure = await m_command.Connection.GetCachedProcedure(ioBehavior, commandText, cancellationToken).ConfigureAwait(false);
			if (cachedProcedure != null)
				parameterCollection = cachedProcedure.AlignParamsWithDb(parameterCollection);

			MySqlParameter? returnParam = null;
			var inParams = new MySqlParameterCollection();
			var argParamNames = new List<string>();
			var inOutSetParams = "";
			for (var i = 0; i < (parameterCollection?.Count ?? 0); i++)
			{
				var param = parameterCollection![i];
				var inName = "@inParam" + i;
				var outName = "@outParam" + i;
				switch (param.Direction)
				{
					case ParameterDirection.Input:
					case ParameterDirection.InputOutput:
						var inParam = param.WithParameterName(inName);
						inParams.Add(inParam);
						if (param.Direction == ParameterDirection.InputOutput)
						{
							inOutSetParams += $"SET {outName}={inName}; ";
							goto case ParameterDirection.Output;
						}
						argParamNames.Add(inName);
						break;
					case ParameterDirection.Output:
						m_outParams.Add(param);
						m_outParamNames.Add(outName);
						argParamNames.Add(outName);
						break;
					case ParameterDirection.ReturnValue:
						returnParam = param;
						break;
				}
			}

			// if a return param is set, assume it is a funciton.  otherwise, assume stored procedure
			commandText += "(" + string.Join(", ", argParamNames) +")";
			if (returnParam == null)
			{
				commandText = inOutSetParams + "CALL " + commandText;
				if (m_outParams.Count > 0)
				{
					m_setParamsFlags = true;
					m_cancellationToken = cancellationToken;
				}
			}
			else
			{
				commandText = "SELECT " + commandText;
			}

			var reader = (MySqlDataReader) await base.ExecuteReaderAsync(commandText, inParams, behavior, ioBehavior, cancellationToken).ConfigureAwait(false);
			try
			{
				if (returnParam != null && await reader.ReadAsync(ioBehavior, cancellationToken).ConfigureAwait(false))
					returnParam.Value = reader.GetValue(0);
				return reader;
			}
			catch (Exception)
			{
				reader.Dispose();
				throw;
			}
		}

		internal void SetParams()
		{
			if (!m_setParamsFlags)
				return;
			m_setParamsFlags = false;
			var commandText = "SELECT " + string.Join(", ", m_outParamNames);
			using var reader = (MySqlDataReader) base.ExecuteReaderAsync(commandText, new MySqlParameterCollection(), CommandBehavior.Default, IOBehavior.Synchronous, m_cancellationToken).GetAwaiter().GetResult();
			reader.Read();
			for (var i = 0; i < m_outParams.Count; i++)
			{
				var param = m_outParams[i];
				if (param.HasSetDbType && !reader.IsDBNull(i))
				{
					var dbTypeMapping = TypeMapper.Instance.GetDbTypeMapping(param.DbType);
					if (dbTypeMapping != null)
					{
						param.Value = dbTypeMapping.DoConversion(reader.GetValue(i));
						continue;
					}
				}
				param.Value = reader.GetValue(i);
			}
		}

		readonly MySqlCommand m_command;
		bool m_setParamsFlags;
		MySqlParameterCollection m_outParams;
		List<string> m_outParamNames;
		private CancellationToken m_cancellationToken;
	}
}
