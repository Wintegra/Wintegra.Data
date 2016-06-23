using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using java.sql;
using Connection = java.sql.Connection;

namespace Wintegra.JDBC.Db2Client
{
	public sealed class Db2Transaction : DbTransaction
	{
		private const IsolationLevel DefaultIsolationLevel = IsolationLevel.ReadCommitted;


		#region Fields and Properties

		public new Db2Connection Connection { get; internal set; }

		public bool IsCompleted { get { return Connection == null; } }

		public override IsolationLevel IsolationLevel
		{
			get
			{
				CheckReady();
				return _isolationLevel;
			}
		}
		private readonly IsolationLevel _isolationLevel;

		protected override DbConnection DbConnection { get { return Connection; } }

		private bool _disposed = false;


		#endregion

		#region Constructors

		internal Db2Transaction(Db2Connection conn)
			: this (conn, DefaultIsolationLevel)
		{
			Debug.Assert(conn != null);
		}

		internal Db2Transaction(Db2Connection conn, IsolationLevel isolationLevel)
		{
			Debug.Assert(conn != null);
			Connection = conn;

			conn.TransactionStatus = TransactionStatus.Pending;
			var connector = CheckReady();

			switch (isolationLevel)
			{
				case IsolationLevel.Chaos:
					conn.TransactionStatus = TransactionStatus.InTransactionBlock;
					connector.setTransactionIsolation(0);  // java.sql.Connection.TRANSACTION_NONE
					connector.setAutoCommit(false);
					break;
				case IsolationLevel.ReadUncommitted:
					conn.TransactionStatus = TransactionStatus.InTransactionBlock;
					connector.setTransactionIsolation(1); // java.sql.Connection.TRANSACTION_READ_UNCOMMITTED
					connector.setAutoCommit(false);
					break;
				case IsolationLevel.ReadCommitted:
					conn.TransactionStatus = TransactionStatus.InTransactionBlock;
					connector.setTransactionIsolation(2); // java.sql.Connection.TRANSACTION_READ_COMMITTED
					connector.setAutoCommit(false);
					break;
				case IsolationLevel.RepeatableRead:
					conn.TransactionStatus = TransactionStatus.InTransactionBlock;
					connector.setTransactionIsolation(4); // java.sql.Connection.TRANSACTION_REPEATABLE_READ
					connector.setAutoCommit(false);
					break;
				case IsolationLevel.Serializable:
					conn.TransactionStatus = TransactionStatus.InTransactionBlock;
					connector.setTransactionIsolation(8);  // java.sql.Connection.TRANSACTION_SERIALIZABLE
					connector.setAutoCommit(false);
					break;
				case IsolationLevel.Unspecified:
					isolationLevel = DefaultIsolationLevel;
					goto case DefaultIsolationLevel;

				case IsolationLevel.Snapshot:
				default:
					throw new ArgumentOutOfRangeException("isolationLevel", isolationLevel, "IBM DB2 is not compatible with the transaction isolation level");
			}
			
			_isolationLevel = isolationLevel;
		}

		#endregion

		#region Commit

		public override void Commit()
		{
			try
			{
				var connector = CheckReady();
				connector.commit();
				connector.setAutoCommit(true);
				Connection.TransactionStatus = TransactionStatus.Idle;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
			finally
			{
				Connection = null;
			}
		}

		#endregion

		#region Rollback

		public override void Rollback()
		{
			try
			{
				var connector = CheckReady();
				connector.rollback();
				connector.setAutoCommit(true);
				Connection.TransactionStatus = TransactionStatus.Idle;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
			finally
			{
				Connection = null;
			}
		}

		#endregion

		#region Dispose

		/// <summary>
		/// Dispose.
		/// </summary>
		/// <param name="disposing"></param>
		protected override void Dispose(bool disposing)
		{
			if (_disposed) { return; }

			if (disposing && Connection != null)
			{
				Rollback();
			}

			_disposed = true;
			base.Dispose(disposing);
		}

		#endregion

		#region Checks

		Connection CheckReady()
		{
			CheckDisposed();
			CheckCompleted();
			return Connection.CheckReadyAndGetConnector();
		}

		private void CheckCompleted()
		{
			if (IsCompleted)
				throw new InvalidOperationException("This Db2Transaction has completed; it is no longer usable.");
		}

		private void CheckDisposed()
		{
			if (_disposed)
				throw new ObjectDisposedException(typeof(Db2Transaction).Name);
		}

		#endregion

	}
}