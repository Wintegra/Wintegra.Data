using System;
using System.Configuration;
using System.Data;
using System.Data.Odbc;
using Microsoft.Win32;

namespace Wintegra.Data.Db2Client
{
	public class Db2Connection : IDbConnection
	{
		public static explicit operator OdbcConnection(Db2Connection connection)
		{
			return connection._connection;
		}

		internal Db2ConnectionStringBuilder Settings { get; private set; }
		private readonly OdbcConnection _connection;

		public Db2Connection()
		{
			_connection = new OdbcConnection();
		}

		public Db2Connection(string connectionRawString)
		{
			Settings = GetConnectionString(connectionRawString);
			_connection = new OdbcConnection(Settings.ConnectionString);
		}

		private static Db2ConnectionStringBuilder GetConnectionString(string connectionRawString)
		{
			var connectionString = connectionRawString;
			var name = Environment.MachineName.ToLower();
			var connectionStringObject = new ConnectionStringSettings("db2_" + name, connectionString);
			connectionStringObject.ProviderName = "System.Data.Odbc";

			if (connectionStringObject.ProviderName.Equals("System.Data.Odbc") && connectionString.Contains("{IBM DB2 ODBC DRIVER}"))
			{
				var odbcDriverNames = GetOdbcDriverNames();
				if (odbcDriverNames == null)
				{
					throw new ArgumentNullException("connectionString", "Not found 'ODBC Drivers'");
				}

				bool containsOdbc = false;
				foreach (var odbcDriverName in odbcDriverNames)
				{
					if (odbcDriverName.Equals("IBM DB2 ODBC DRIVER"))
					{
						containsOdbc = true;
						break;
					}
				}

				if (!containsOdbc)
				{
					foreach (var odbcDriverName in odbcDriverNames)
					{
						if (odbcDriverName.StartsWith("IBM DB2 ODBC DRIVER -") || odbcDriverName.StartsWith("IBM DATA SERVER DRIVER for ODBC -"))
						{
							connectionString = connectionString.Replace("{IBM DB2 ODBC DRIVER}", "{" + odbcDriverName + "}");
							break;
						}
					}
				}
			}
			return new Db2ConnectionStringBuilder(connectionString);
		}

		private static string[] GetOdbcDriverNames()
		{
			string[] odbcDriverNames = null;
			using (var localMachineHive = Registry.LocalMachine)
			using (var odbcDriversKey = localMachineHive.OpenSubKey(@"SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers"))
			{
				if (odbcDriversKey != null)
				{
					odbcDriverNames = odbcDriversKey.GetValueNames();
				}
			}
			return odbcDriverNames;
		}

		public void Dispose()
		{
			try
			{
				_connection.Dispose();
			}
			catch (InvalidOperationException)
			{
				// Ignore
			}
		}

		public IDbTransaction BeginTransaction()
		{
			return _connection.BeginTransaction();
		}

		public IDbTransaction BeginTransaction(IsolationLevel il)
		{
			return _connection.BeginTransaction(il);
		}

		public void Close()
		{
			_connection.Close();
		}

		public void ChangeDatabase(string databaseName)
		{
			_connection.ChangeDatabase(databaseName);
		}

		public IDbCommand CreateCommand()
		{
			var command = new Db2Command(_connection.CreateCommand());
			command.CommandTimeout = Settings.CommandTimeout;
			return command;
		}

		public void Open()
		{
			_connection.Open();
		}

		public string ConnectionString
		{
			get { return _connection.ConnectionString; }
			set
			{
				Settings = GetConnectionString(value);
				_connection.ConnectionString = Settings.ConnectionString;
			}
		}

		public int ConnectionTimeout
		{
			get { return _connection.ConnectionTimeout; }
			set { _connection.ConnectionTimeout = value; }
		}

		public string Database
		{
			get { return _connection.Database; }
		}

		public ConnectionState State
		{
			get { return _connection.State; }
		}
	}
}