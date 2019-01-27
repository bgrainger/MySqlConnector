#if !NETSTANDARD1_3
using System;
using System.Transactions;
using MySql.Data.MySqlClient;

namespace MySqlConnector.Core
{
	internal abstract class ImplicitTransactionBase : IEnlistmentNotification
	{
		public MySqlConnection Connection { get; }

		public void Start(Transaction transaction)
		{
			Transaction = transaction;
			OnStart();
			Transaction.EnlistVolatile(this, EnlistmentOptions.None);
		}

		void IEnlistmentNotification.Prepare(PreparingEnlistment enlistment)
		{
			OnPrepare(enlistment);
			enlistment.Prepared();
		}

		void IEnlistmentNotification.Commit(Enlistment enlistment)
		{
			OnCommit(enlistment);
			enlistment.Done();
			Connection.UnenlistTransaction(this, Transaction!);
			Transaction = null;
		}

		void IEnlistmentNotification.Rollback(Enlistment enlistment)
		{
			OnRollback(enlistment);
			enlistment.Done();
			Connection.UnenlistTransaction(this, Transaction!);
			Transaction = null;
		}

		public void InDoubt(Enlistment enlistment) => throw new NotImplementedException();

		protected ImplicitTransactionBase(MySqlConnection connection) => Connection = connection;

		protected Transaction? Transaction { get; private set; }

		protected abstract void OnStart();
		protected abstract void OnPrepare(PreparingEnlistment enlistment);
		protected abstract void OnCommit(Enlistment enlistment);
		protected abstract void OnRollback(Enlistment enlistment);
	}
}
#endif
