using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Xml;
using CallableStatement = java.sql.CallableStatement;
using ResultSet = java.sql.ResultSet;
using SQLException = java.sql.SQLException;

namespace Wintegra.Data.jdbc
{
	internal sealed class Db2DataReader : DbDataReader
	{
		enum ColumnDb2TypeEnum
		{
			Default = 0,
			Blob,
			Clob,
			Xml,
			Int16,
			Int32,
			Int64,
			Decimal,
			Float,
			Double,
			Time,
			Date,
			Timestamp,
		}

		private bool _disposed = false;
		private readonly Db2Connection _connection;
		private readonly Db2Command _command;

		private int _currentCommand;
		private readonly string[] _batchCommands;
		private readonly Db2ParameterCollection[] _batchParameters;

		private CallableStatement _statement;
		private ResultSet _rs;

		/// <exception cref="Db2Exception"></exception>
		internal Db2DataReader(Db2Command command)
		{
			_connection = command.Connection;
			_command = command;

			_currentCommand = -1;
			_batchCommands = PrepareBatchCommands(_command.CommandText);
			_batchParameters = PrepareBatchParameters(_command.Parameters);
			_statement = PrepareStatementAndResultSet();
			_rs = _statement.executeQuery();
		}

		internal static readonly Regex QueryMultipleExp = new Regex(@"(?<q>[^;]+);?", RegexOptions.Compiled | RegexOptions.IgnoreCase);
		private static string[] PrepareBatchCommands(string commandText)
		{
			var queryMultiple = QueryMultipleExp.Matches(commandText);
			var batchCommands = new string[queryMultiple.Count];

			for (int i = 0; i < queryMultiple.Count; i++)
			{
				batchCommands[i] = queryMultiple[i].Groups["q"].Value;
			}
			return batchCommands;
		}

		internal static readonly Regex ParameterNameExp = new Regex(@"(?<n>:\w+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
		private Db2ParameterCollection[] PrepareBatchParameters(Db2ParameterCollection parameters)
		{
			if (_batchCommands.Length < 2) return new Db2ParameterCollection[] { parameters };

			var batchParameters = new Db2ParameterCollection[_batchCommands.Length];
			int parameter = 0;
			for (int i = 0; i < _batchCommands.Length; i++)
			{
				var query = _batchCommands[i];
				var commandText = Db2Command.PrepareCommandText(query, parameters, ref parameter);
				_batchCommands[i] = commandText;
				var names = ParameterNameExp.Matches(commandText);

				var collection = new Db2ParameterCollection();
				foreach (Match n in names)
				{
					var name = n.Groups["n"].Value;
					var p = parameters[name].Clone();
					collection.Add(p);
				}

				batchParameters[i] = collection;
			}
			return batchParameters;
		}

		private CallableStatement PrepareStatementAndResultSet()
		{
			_currentCommand++;
			if (_currentCommand >= _batchCommands.Length) return null;
			var query = _batchCommands[_currentCommand];
			var parameters = _batchParameters[_currentCommand];
			return Db2Command.PrepareExecute(_connection, query, parameters);
		}

		~Db2DataReader()
		{
			Dispose(false);
		}

		public override void Close()
		{
			Dispose(true);
		}

		public override DataTable GetSchemaTable()
		{
			throw new NotImplementedException();
		}

		public override bool NextResult()
		{
			try
			{
				_rs.close();
				_rs = null;
				_rowDescription = null;
				var b = _statement.getMoreResults();
				if (b)
				{
					_rs = _statement.getResultSet();
				}
				else
				{
					_statement = PrepareStatementAndResultSet();
					if (_statement != null)
					{
						_rs = _statement.executeQuery();
						b = true;
					}
				}
				return b;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override bool Read()
		{
			try
			{
				return _rs.next();
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override int Depth
		{
			get { throw new NotImplementedException(); }
		}

		public override bool IsClosed { get { return (_rs == null) || _rs.isClosed(); } }

		public override int RecordsAffected
		{
			get { throw new NotImplementedException(); }
		}

		public override bool GetBoolean(int ordinal)
		{
			try
			{
				return _rs.getBoolean(ordinal + 1);
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override byte GetByte(int ordinal)
		{
			try
			{
				return _rs.getByte(ordinal + 1);
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
		{
			throw new NotImplementedException();
		}

		public override char GetChar(int ordinal)
		{
			try
			{
				return _rs.getString(ordinal + 1)[0];
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
		{
			throw new NotImplementedException();
		}

		public override Guid GetGuid(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override short GetInt16(int ordinal)
		{
			var d = GetShort(ordinal);
			if (!d.HasValue) throw new NullReferenceException("DB2 returned null");
			return d.Value;
		}

		public short? GetShort(int ordinal)
		{
			try
			{
				var obj = _rs.getObject(ordinal + 1);
				{
					var value = obj as java.lang.Short;
					if (value != null) return value.shortValue();
				}
				{
					// !! important JDBC returns Int32
					var value = obj as java.lang.Integer;
					if (value != null) return value.shortValue();
				}


				return null;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override int GetInt32(int ordinal)
		{
			var d = GetInteger(ordinal);
			if (!d.HasValue) throw new NullReferenceException("DB2 returned null");
			return d.Value;
		}

		public int? GetInteger(int ordinal)
		{
			try
			{
				var obj = _rs.getObject(ordinal + 1);
				var value = obj as java.lang.Integer;
				if (value != null) return value.intValue();

				return null;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override long GetInt64(int ordinal)
		{
			var d = GetLong(ordinal);
			if (!d.HasValue) throw new NullReferenceException("DB2 returned null");
			return d.Value;
		}

		public long? GetLong(int ordinal)
		{
			try
			{
				var obj = _rs.getObject(ordinal + 1);
				var value = obj as java.lang.Long;
				if (value != null) return value.longValue();

				return null;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override DateTime GetDateTime(int ordinal)
		{
			var d = GetDateTimeOrNull(ordinal);
			if (!d.HasValue) throw new NullReferenceException("DB2 returned null");
			return d.Value;
		}

		public DateTime? GetDateTimeOrNull(int ordinal)
		{
			try
			{
				var obj = _rs.getObject(ordinal + 1);
				{
					var value = obj as java.sql.Date;
					if (value != null) return Db2Util.ToDateTime(value);
				}
				{
					var value = obj as java.sql.Timestamp;
					if (value != null) return Db2Util.ToDateTime(value);
				}

				return null;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public TimeSpan? GetTime(int ordinal)
		{
			try
			{
				var obj = _rs.getObject(ordinal + 1);
				var value = obj as java.sql.Time;
				if (value != null) return Db2Util.ToTimeSpan(value);

				return null;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override string GetString(int ordinal)
		{
			try
			{
				return _rs.getString(ordinal + 1);
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override decimal GetDecimal(int ordinal)
		{
			var d = GetBigDecimal(ordinal);
			if (!d.HasValue) throw new NullReferenceException("DB2 returned null");
			return (decimal)d.Value;
		}

		private decimal? GetBigDecimal(int ordinal)
		{
			try
			{
				var obj = _rs.getObject(ordinal + 1);
				var value = obj as java.math.BigDecimal;
				if (value != null) return Db2Util.ToDecimal(value);

				return null;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override float GetFloat(int ordinal)
		{
			var d = GetReal(ordinal);
			if (!d.HasValue) throw new NullReferenceException("DB2 returned null");
			return d.Value;
		}

		private float? GetReal(int ordinal)
		{
			try
			{
				var obj = _rs.getObject(ordinal + 1);
				var value = obj as java.lang.Float;
				if (value != null) return value.floatValue();

				return null;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override double GetDouble(int ordinal)
		{
			var d = GetDoubleOrNull(ordinal);
			if (!d.HasValue) throw new NullReferenceException("DB2 returned null");
			return d.Value;
		}

		private double? GetDoubleOrNull(int ordinal)
		{
			try
			{
				var obj = _rs.getObject(ordinal + 1);
				var value = obj as java.lang.Double;
				if (value != null) return value.doubleValue();

				return null;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public XmlDocument GetXmlDocument(int ordinal)
		{
			object obj;
			try
			{
				obj = _rs.getObject(ordinal + 1);
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
			
			var sqlxml = obj as java.sql.SQLXML;
			if (sqlxml == null) return null;
			var xml = new XmlDocument();
			xml.LoadXml(sqlxml.getString());
			var xmlDeclaration = xml.CreateXmlDeclaration("1.0", "UTF-16", null);
			XmlElement root = xml.DocumentElement;
			xml.InsertBefore(xmlDeclaration, root);
			return xml;
		}

		private byte[] GetBlob(int ordinal)
		{
			object obj;
			try
			{
				obj = _rs.getObject(ordinal + 1);
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
			var blob = obj as java.sql.Blob;
			if (blob == null) return null;
			return blob.getBytes(1, (int)blob.length());
		}

		class RowDescription
		{
			public int FieldCount { get; set; }
			public string[] ColumnNames { get; set; }
			public ColumnDb2TypeEnum[] ColumnDb2Types { get; set; }
			public Type[] ColumnTypes { get; set; }

			public RowDescription(int columnCount)
			{
				FieldCount = columnCount;
				ColumnNames = new string[columnCount];
				ColumnDb2Types = new ColumnDb2TypeEnum[columnCount];
				ColumnTypes = new Type[columnCount];
			}
		}

		private readonly object _syncRoot = new object();
		private RowDescription _rowDescription;

		private void EnsureTypes()
		{
			if (_rowDescription != null) return;
			lock (_syncRoot)
			{
				var meta = _rs.getMetaData();
				var columnCount = meta.getColumnCount();

				var rowDescription = new RowDescription(columnCount);
				for (int column = 0; column < columnCount; column++)
				{
					var ordinal = column + 1;

					rowDescription.ColumnNames[column] = meta.getColumnLabel(ordinal);

					ColumnDb2TypeEnum columnDb2Type;
					Type columnType;
					var columnTypeName = meta.getColumnTypeName(ordinal);
					switch (columnTypeName)
					{
						case "CHARACTER":
						case "VARCHAR":
						case "CLOB":
						case "GRAPHIC":
						case "VARGRAPHIC":
						case "DBCLOB":
							columnDb2Type = ColumnDb2TypeEnum.Clob;
							columnType = typeof(string);
							break;
						case "BLOB":
							columnDb2Type = ColumnDb2TypeEnum.Blob;
							columnType = typeof (byte[]);
							break;
						case "SMALLINT":
							columnDb2Type = ColumnDb2TypeEnum.Int16;
							columnType = typeof (short);
							break;
						case "INTEGER":
							columnDb2Type = ColumnDb2TypeEnum.Int32;
							columnType = typeof (int);
							break;
						case "BIGINT":
							columnDb2Type = ColumnDb2TypeEnum.Int64;
							columnType = typeof (long);
							break;
						case "DECFLOAT":
						case "DECIMAL":
							columnDb2Type = ColumnDb2TypeEnum.Decimal;
							columnType = typeof(decimal);
							break;
						case "REAL":
							columnDb2Type = ColumnDb2TypeEnum.Float;
							columnType = typeof(float);
							break;
						case "DOUBLE":
							columnDb2Type = ColumnDb2TypeEnum.Double;
							columnType = typeof(double);
							break;

						case "DATE":
							columnDb2Type = ColumnDb2TypeEnum.Date;
							columnType = typeof(DateTime);
							break;
						case "TIME":
							columnDb2Type = ColumnDb2TypeEnum.Time;
							columnType = typeof(TimeSpan);
							break;
						case "TIMESTAMP":
							columnDb2Type = ColumnDb2TypeEnum.Timestamp;
							columnType = typeof(DateTime);
							break;


						case "XML":
							columnDb2Type = ColumnDb2TypeEnum.Xml;
							columnType = typeof(XmlDocument);
							break;

						default:
							columnDb2Type = ColumnDb2TypeEnum.Default;
							columnType = typeof (object);
							break;
					}



					rowDescription.ColumnDb2Types[column] = columnDb2Type;
					rowDescription.ColumnTypes[column] = columnType;
				}

				_rowDescription = rowDescription;
			}
		}

		public override object GetValue(int ordinal)
		{
			try
			{
				EnsureTypes();
				switch (_rowDescription.ColumnDb2Types[ordinal])
				{
					case ColumnDb2TypeEnum.Blob:
						return GetBlob(ordinal);
					case ColumnDb2TypeEnum.Clob:
						return GetString(ordinal);
					case ColumnDb2TypeEnum.Xml:
						return GetXmlDocument(ordinal);
					case ColumnDb2TypeEnum.Int16:
						{
							var d = GetShort(ordinal);
							if (d.HasValue) return d.Value;
						}
						return DBNull.Value;
					case ColumnDb2TypeEnum.Int32:
						{
							var d = GetInteger(ordinal);
							if (d.HasValue) return d.Value;
						}
						return DBNull.Value;
					case ColumnDb2TypeEnum.Int64:
						{
							var d = GetLong(ordinal);
							if (d.HasValue) return d.Value;
						}
						return DBNull.Value;
					case ColumnDb2TypeEnum.Decimal:
						{
							var d = GetBigDecimal(ordinal);
							if (d.HasValue) return (decimal)d.Value;
						}
						return DBNull.Value;
					case ColumnDb2TypeEnum.Float:
						{
							var d = GetReal(ordinal);
							if (d.HasValue) return d.Value;
						}
						return DBNull.Value;
					case ColumnDb2TypeEnum.Double:
						{
							var d = GetDoubleOrNull(ordinal);
							if (d.HasValue) return d.Value;
						}
						return DBNull.Value;
					case ColumnDb2TypeEnum.Time:
						{
							var d = GetTime(ordinal);
							if (d.HasValue) return d.Value;
						}
						return DBNull.Value;
					case ColumnDb2TypeEnum.Date:
						{
							var d = GetDateTimeOrNull(ordinal);
							if (d.HasValue) return d.Value;
						}
						return DBNull.Value;
					case ColumnDb2TypeEnum.Timestamp:
						{
							var d = GetDateTimeOrNull(ordinal);
							if (d.HasValue) return d.Value;
						}
						return DBNull.Value;
					case ColumnDb2TypeEnum.Default:
					default:
						var value = _rs.getObject(ordinal + 1);
						return value ?? DBNull.Value;
				}
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override int GetValues(object[] values)
		{
			throw new NotImplementedException();
		}

		public override bool IsDBNull(int ordinal)
		{
			EnsureTypes();
			try
			{
				return _rs.getObject(ordinal + 1) == null;
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		public override int FieldCount
		{
			get
			{
				EnsureTypes();
				return _rowDescription.FieldCount;
			}
		}

		public override object this[int ordinal] { get { return GetValue(ordinal); } }

		public override object this[string name]
		{
			get
			{
				int ordinal = GetOrdinal(name);
				return GetValue(ordinal);
			}
		}

		public override bool HasRows
		{
			get { throw new NotImplementedException(); }
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing)
		{
			if (_disposed) return;

			if (_rs != null)
			{
				try
				{
					_rs.close();
				}
				catch (SQLException ignore) 
				{
				}
				_rs = null;
			}

			_disposed = true;
		}

		public override string GetName(int ordinal)
		{
			EnsureTypes();
			return _rowDescription.ColumnNames[ordinal];
		}

		private int _nextNameNo = 0;
		public override int GetOrdinal(string name)
		{
			EnsureTypes();
			var n = name.ToUpper();
			int? j = null;
			for (int p = 0; p < FieldCount; p++)
			{
				int i = (_nextNameNo + p) % FieldCount;
				if (_rowDescription.ColumnNames[i].Equals(n))
				{
					j = i;
					break;
				}
			}
			if (!j.HasValue)
			{
				throw new ArgumentOutOfRangeException("name");
			}
			_nextNameNo = j.Value + 1;
			return j.Value;
		}

		public override string GetDataTypeName(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override Type GetFieldType(int ordinal)
		{
			EnsureTypes();
			return _rowDescription.ColumnTypes[ordinal];
		}

		public override IEnumerator GetEnumerator()
		{
			throw new NotImplementedException();
		}
	}
}