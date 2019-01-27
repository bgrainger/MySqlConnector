using System;
using System.Collections;
using System.Data.Common;
#if !NETSTANDARD1_3
using System.Runtime.Serialization;
#endif

namespace MySql.Data.MySqlClient
{
#if !NETSTANDARD1_3
	[Serializable]
#endif
	public sealed class MySqlException : DbException
	{
		public int Number { get; }
		public string? SqlState { get; }

#if !NETSTANDARD1_3
		private MySqlException(SerializationInfo info, StreamingContext context)
			: base(info, context)
		{
			Number = info.GetInt32("Number");
			SqlState = info.GetString("SqlState");
		}

		public override void GetObjectData(SerializationInfo info, StreamingContext context)
		{
			base.GetObjectData(info, context);
			info.AddValue("Number", Number);
			info.AddValue("SqlState", SqlState);
		}
#endif

		public override IDictionary Data
		{
			get
			{
				if (m_data == null)
				{
					m_data = base.Data;
					m_data["Server Error Code"] = Number;
					m_data["SqlState"] = SqlState;
				}
				return m_data;
			}
		}

		internal MySqlException(string message)
			: this(message, null)
		{
		}

		internal MySqlException(string message, Exception? innerException)
			: this(0, null, message, innerException)
		{
		}

		internal MySqlException(int errorNumber, string? sqlState, string message)
			: this(errorNumber, sqlState, message, null)
		{
		}

		internal MySqlException(int errorNumber, string? sqlState, string message, Exception? innerException)
			: base(message, innerException)
		{
			Number = errorNumber;
			SqlState = sqlState;
		}

		internal static MySqlException CreateForTimeout() => CreateForTimeout(null);

		internal static MySqlException CreateForTimeout(Exception? innerException) =>
			new MySqlException((int) MySqlErrorCode.CommandTimeoutExpired, null, "The Command Timeout expired before the operation completed.", innerException);

		IDictionary? m_data;
	}
}
