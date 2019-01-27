#if !NETSTANDARD1_3
using System.Globalization;
using System.Threading;
using System.Transactions;
using MySql.Data.MySqlClient;

namespace MySqlConnector.Core
{
	internal sealed class XaImplicitTransaction : ImplicitTransactionBase
	{
		public XaImplicitTransaction(MySqlConnection connection)
			: base(connection)
		{
		}

		protected override void OnStart()
		{
			// generate an "xid" with "gtrid" (Global TRansaction ID) from the .NET Transaction and "bqual" (Branch QUALifier)
			// unique to this object
			var id = Interlocked.Increment(ref s_currentId);
			m_xid = "'" + Transaction!.TransactionInformation.LocalIdentifier + "', '" + id.ToString(CultureInfo.InvariantCulture) + "'";

			ExecuteXaCommand("START");

			// TODO: Support EnlistDurable and enable recovery via "XA RECOVER"
		}

		protected override void OnPrepare(PreparingEnlistment enlistment)
		{
			ExecuteXaCommand("END");
			ExecuteXaCommand("PREPARE");
		}

		protected override void OnCommit(Enlistment enlistment)
		{
			ExecuteXaCommand("COMMIT");
		}

		protected override void OnRollback(Enlistment enlistment)
		{
			ExecuteXaCommand("END");
			ExecuteXaCommand("ROLLBACK");
		}

		private void ExecuteXaCommand(string statement)
		{
			using var cmd = Connection.CreateCommand();
			cmd.CommandText = "XA " + statement + " " + m_xid;
			cmd.ExecuteNonQuery();
		}

		static int s_currentId;

		string? m_xid;
	}
}
#endif
