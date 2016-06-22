using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using CallableStatement = java.sql.CallableStatement;
using ByteArrayInputStream = java.io.ByteArrayInputStream;
using SQLException = java.sql.SQLException;

namespace Wintegra.Data.jdbc
{
	public sealed class Db2Command : DbCommand, ICloneable
	{
		#region Fields

		private bool _disposed = false;

		private string _commandText;
		private Db2Connection _connection;

		private Db2Transaction _transaction;
		private readonly Db2ParameterCollection _parameters;

		#endregion

		#region Constructors
		
		public Db2Command() : base()
		{
			_parameters = new Db2ParameterCollection();
		}

		internal Db2Command(Db2Connection connection) : this()
		{
			Connection = connection;
		}

		#endregion

		#region Public properties

		internal static readonly Regex XmlExp = new Regex(@"\(:(?<n>\w+)\d+,:\1\d+\)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

		[Category("Data"), DefaultValue("")]
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

		public override UpdateRowSource UpdatedRowSource
		{
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		#endregion

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

		#region Prepare

		public override void Prepare()
		{
			throw new NotImplementedException();
		}

		internal static CallableStatement PrepareExecute(Db2Connection connection, string query, Db2ParameterCollection parameters)
		{
			var connector = connection.CheckReadyAndGetConnector();
			int parameter = 0;
			var commandText = PrepareCommandText(query, parameters, ref parameter);
			var stmt = connector.prepareCall(commandText);
			PrepareParameters(stmt, parameters);

			return stmt;
		}

		internal static string PrepareCommandText(string query, Db2ParameterCollection parameters, ref int parameter)
		{
			if (!query.Contains("?")) return query;

			var sb = new StringBuilder();
			int len = parameters.Count;
			foreach (var ch in query)
			{
				if ('?' != ch)
				{
					sb.Append(ch);
					continue;
				}

			again_search_param:
				var p = parameters[parameter % len];
				var obj = p.Value;
				var name = p.CleanName;
				parameter++;

			again_inject_param:
				if (obj is XmlDeclaration)
				{
					goto again_search_param; // skip for Dapper
				}
				if (obj is XmlElement)
				{
					obj = ((XmlElement)obj).OwnerDocument; // compatible with Dapper
					name = name.Substring(0, name.Length - 1);
					goto again_inject_param;
				}

				sb.Append(":" + name);
			}
			return sb.ToString();
		}

		private static void PrepareParameters(CallableStatement stmt, Db2ParameterCollection parameters)
		{
			foreach (Db2Parameter p in parameters)
			{
				//switch (p.Db2DataType)
				var obj = p.Value;
				var name = p.CleanName;

				again_add_param:
				if (obj is DBNull)
				{
					stmt.setNull(name, java.sql.Types.NVARCHAR);
				}
				else if (obj is string)
				{
					stmt.setString(name, (string) obj);
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
					continue; // skip for Dapper
				}
				else if (obj is XmlElement)
				{
					obj = ((XmlElement) obj).OwnerDocument; // compatible with Dapper
					name = name.Substring(0, name.Length - 1);
					goto again_add_param;
				}
				else if (obj is short)
				{
					stmt.setShort(name, (short) obj);
				}
				else if (obj is int)
				{
					stmt.setInt(name, (int) obj);
				}
				else if (obj is long)
				{
					stmt.setLong(name, (long) obj);
				}
				else if (obj is decimal)
				{
					stmt.setBigDecimal(name, Db2Util.toBigDecimal((decimal) obj));
				}
				else if (obj is float)
				{
					stmt.setFloat(name, (float) obj);
				}
				else if (obj is double)
				{
					stmt.setDouble(name, (double) obj);
				}
				else if (obj is TimeSpan)
				{
					stmt.setTime(name, Db2Util.toTime((TimeSpan) obj));
				}
				else if (obj is DateTime)
				{
					stmt.setTimestamp(name, Db2Util.toTimestamp((DateTime) obj));
				}
				else if (obj is byte[])
				{
					stmt.setBytes(name, (byte[]) obj);
				}
				else if (obj is char)
				{
					stmt.setString(name, obj.ToString());
				}
				else
				{
					stmt.setObject(name, obj);
				}
			}
		}
		
		#endregion

		#region Execute Non Query

		public override int ExecuteNonQuery()
		{
			try
			{
				var stmt = PrepareExecute(_connection, _commandText, Parameters);
				return stmt.executeUpdate();
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		#endregion

		#region Execute Scalar

		public override object ExecuteScalar()
		{
			try
			{
				using (var reader = new Db2DataReader(this))
				{
					return reader.GetValue(0);
				}
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		#endregion

		#region Execute Reader

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
		{
			try
			{
				return new Db2DataReader(this);
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
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

		#region Cancel

		public override void Cancel()
		{
			throw new NotImplementedException();
		}

		#endregion

		#region Dispose

		~Db2Command()
		{
			Dispose(false);
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

		#endregion

		#region Misc

		object ICloneable.Clone()
		{
			return Clone();
		}

		public Db2Command Clone()
		{
			throw new NotImplementedException();
		}

		#endregion
	}
}