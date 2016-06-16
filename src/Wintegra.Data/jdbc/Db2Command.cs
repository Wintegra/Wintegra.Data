using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Xml;
using CallableStatement = java.sql.CallableStatement;
using ByteArrayInputStream = java.io.ByteArrayInputStream;
using SQLException = java.sql.SQLException;

namespace Wintegra.Data.jdbc
{
	public sealed class Db2Command : DbCommand, ICloneable
	{
		private bool _disposed = false;

		private string _commandText;
		private Db2Connection _connection;

		private Db2Transaction _transaction;
		private readonly Db2ParameterCollection _parameters;

		public Db2Command() : base()
		{
			_parameters = new Db2ParameterCollection();
		}

		internal Db2Command(Db2Connection connection) : this()
		{
			Connection = connection;
		}

		~Db2Command()
		{
			Dispose(false);
		}

		public object Clone()
		{
			throw new NotImplementedException();
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing)
		{
			if (_disposed) return;


			_disposed = true;
		}

		public override void Prepare()
		{
			throw new NotImplementedException();
		}

		public static readonly Regex XmlExp = new Regex(@"\(:(?<n>\w+)\d+,:\1\d+\)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

		public override string CommandText
		{
			get { return _commandText; }
			set
			{
				var commandText = Db2Command.XmlExp.Replace(value, m => ":" + m.Groups["n"].Value);
				if (string.Equals(commandText, _commandText)) return;

				_commandText = commandText;
			}
		}

		public override int CommandTimeout
		{
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public override CommandType CommandType
		{
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public override UpdateRowSource UpdatedRowSource
		{
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		protected override DbConnection DbConnection
		{
			get { return Connection; }
			set { Connection = (Db2Connection)value; }
		}

		[Category("Behavior"), DefaultValue(null)]
		public new Db2Connection Connection
		{
			get { return _connection; }
			set
			{
				if (_connection == value)
				{
					return;
				}

				_connection = value;
				Transaction = null;
			}
		}

		public override bool DesignTimeVisible
		{
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public override void Cancel()
		{
			throw new NotImplementedException();
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
		{
			try
			{
				var stmt = PrepareExecute();
				var resultSet = stmt.executeQuery();
				return new Db2DataReader(stmt, resultSet);
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		private CallableStatement PrepareExecute()
		{
			var connector = _connection.CheckReadyAndGetConnector();
			var stmt = connector.prepareCall(_commandText);
			
			foreach (Db2Parameter p in Parameters)
			{
				//switch (p.Db2DataType)
				var obj = p.Value;
				var name = p.CleanName;

			again:
				if (obj is byte[])
				{
					stmt.setObject(name, obj);
				}
				else if (obj is XmlDocument)
				{
					var xml = ((XmlDocument) obj);
					var encoding = System.Text.Encoding.UTF8;
					if (xml.HasChildNodes)
					{
						var xmlDeclaration = xml.FirstChild as XmlDeclaration;
						if (xmlDeclaration != null)
						{
							if (!string.IsNullOrEmpty(xmlDeclaration.Encoding))
							{
								encoding = System.Text.Encoding.GetEncoding(xmlDeclaration.Encoding);
							}
						}
					}
					var buf = encoding.GetBytes(xml.OuterXml);
					using (var stream = new ByteArrayInputStream(buf))
					{
						stmt.setBinaryStream(name, stream);
						stream.close();
					}
				}
				else if (obj is XmlDeclaration)
				{
					continue;  // skip for Dapper
				}
				else if (obj is XmlElement)
				{
					obj = ((XmlElement)obj).OwnerDocument; // compatible with Dapper
					name = name.Substring(0, name.Length - 1);
					goto again;
				}
				else
				{
					stmt.setString(name, (string)obj);
				}
			}
			return stmt;
		}

		public override int ExecuteNonQuery()
		{
			try
			{
				var stmt = PrepareExecute();
				return stmt.executeUpdate();
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override object ExecuteScalar()
		{
			throw new NotImplementedException();
		}


		#region Parameters

		protected override DbParameter CreateDbParameter()
		{
			return CreateParameter();
		}

		public new Db2Parameter CreateParameter()
		{
			return new Db2Parameter();
		}

		protected override DbParameterCollection DbParameterCollection { get { return Parameters; } }

		[Category("Data"), DesignerSerializationVisibility(DesignerSerializationVisibility.Content)]
		public new Db2ParameterCollection Parameters
		{
			get
			{
				return _parameters;
			}
		}

		#endregion





		#region Transactions

		protected override DbTransaction DbTransaction
		{
			get { return Transaction; }
			set { Transaction = (Db2Transaction)value; }
		}

		[Browsable(false), DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden)]
		public new Db2Transaction Transaction
		{
			get
			{
				if (_transaction != null && _transaction.Connection == null)
				{
					_transaction = null;
				}
				return _transaction;
			}
			set { _transaction = value; }
		}



		#endregion
	}
}