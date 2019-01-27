#if !NETSTANDARD1_3
using System.Transactions;
using MySql.Data.MySqlClient;

namespace MySqlConnector.Core
{
	internal sealed class StandardImplicitTransaction : ImplicitTransactionBase
	{
		public StandardImplicitTransaction(MySqlConnection connection) : base(connection)
		{
		}

		protected override void OnStart()
		{
			System.Data.IsolationLevel isolationLevel = Transaction!.IsolationLevel switch
			{
				IsolationLevel.Serializable => System.Data.IsolationLevel.Serializable,
				IsolationLevel.RepeatableRead => System.Data.IsolationLevel.RepeatableRead,
				IsolationLevel.ReadCommitted => System.Data.IsolationLevel.ReadCommitted,
				IsolationLevel.ReadUncommitted => System.Data.IsolationLevel.ReadUncommitted,
				IsolationLevel.Snapshot => System.Data.IsolationLevel.Snapshot,
				IsolationLevel.Chaos => System.Data.IsolationLevel.Chaos,
				IsolationLevel.Unspecified => System.Data.IsolationLevel.Unspecified,
				_ => System.Data.IsolationLevel.Unspecified
			};
			m_transaction = Connection.BeginTransaction(isolationLevel);
		}

		protected override void OnPrepare(PreparingEnlistment enlistment)
		{
		}

		protected override void OnCommit(Enlistment enlistment)
		{
			m_transaction!.Commit();
			m_transaction = null;
		}

		protected override void OnRollback(Enlistment enlistment)
		{
			m_transaction!.Rollback();
			m_transaction = null;
		}

		MySqlTransaction? m_transaction;
	}
}
#endif
