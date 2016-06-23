using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using Thread = java.lang.Thread;
using Class = java.lang.Class;
using DriverManager = java.sql.DriverManager;
using Connection = java.sql.Connection;
using SQLException = java.sql.SQLException;

namespace Wintegra.JDBC.Db2Client
{
	public sealed class Db2Connection : DbConnection, ICloneable
	{
		#region Fields

		private bool _disposed = false;
		internal Db2ConnectionStringBuilder Settings { get; private set; }
		private string _connectionString;
		private Connection _connector;

		#endregion

		#region Constructors / Init / Open

		static Db2Connection()
		{
			Class.forName(typeof(com.ibm.db2.jcc.DB2Driver).AssemblyQualifiedName, true, Thread.currentThread().getContextClassLoader());
		}

		public Db2Connection()
		{
		}

		public Db2Connection(string connectionString)
			: this()
		{
			ConnectionString = connectionString;
		}

		public override void Open()
		{
			_connector = DriverManager.getConnection(_connectionString);
			OnStateChange(new StateChangeEventArgs(ConnectionState.Closed, ConnectionState.Open));
		}


		#endregion

		#region Connection string management

		public override string ConnectionString
		{
			get { return _connectionString; }
			set
			{
				if (value == null)
				{
					value = string.Empty;
				}
				Settings = new Db2ConnectionStringBuilder(value);
				_connectionString = Settings.ConnectionString;
			}
		}
		
		#endregion

		#region Configuration settings

		public override int ConnectionTimeout
		{
			get { throw new NotImplementedException(); }
		}

		public override string Database
		{
			get { throw new NotImplementedException(); }
		}

		public override string DataSource
		{
			get { throw new NotImplementedException(); }
		}

		#endregion

		#region State management

		[Browsable(false)]
		public override ConnectionState State
		{
			get
			{
				if (_connector == null || _disposed) return ConnectionState.Closed;
				return _connector.isClosed() ? ConnectionState.Closed : ConnectionState.Open;
			}
		}

		internal TransactionStatus TransactionStatus { get; set; }

		internal Connection CheckReadyAndGetConnector()
		{
			if (_disposed)
				throw new ObjectDisposedException(typeof(Db2Connection).Name);
			if (_connector == null)
				throw new InvalidOperationException("Connection is not open");
			return _connector;
		}

		#endregion

		#region Commands

		protected override DbCommand CreateDbCommand()
		{
			return CreateCommand();
		}

		public new Db2Command CreateCommand()
		{
			return new Db2Command(this);
		}

		#endregion

		#region Transactions

		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
		{
			return new Db2Transaction(this, isolationLevel);
		}

		#endregion

		#region Close

		public override void Close()
		{
			if (_connector == null) return;
			try
			{
				_connector.close();
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
			_connector = null;
			OnStateChange(new StateChangeEventArgs(ConnectionState.Open, ConnectionState.Closed));
		}

		~Db2Connection()
		{
			Dispose(false);
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected override void Dispose(bool disposing)
		{
			if (_disposed) return;

			try
			{
				Close();
			}
			catch (Db2Exception ignore)
			{
			}

			base.Dispose(disposing);
			_disposed = true;
		}

		#endregion

		#region Backend version and capabilities

		public override string ServerVersion
		{
			get { throw new NotImplementedException(); }
		}

		#endregion

		#region Misc

		public object Clone()
		{
			throw new NotImplementedException();
		}

		public override void ChangeDatabase(string databaseName)
		{
			throw new NotImplementedException();
		}

		#endregion
	}
}