using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Xml;
using Statement = java.sql.Statement;
using ResultSet = java.sql.ResultSet;
using ResultSetMetaData = java.sql.ResultSetMetaData;
using SQLException = java.sql.SQLException;

namespace Wintegra.Data.jdbc
{
	internal sealed class Db2DataReader : DbDataReader
	{
		private bool _disposed = false;
		private readonly Statement _statement;
		private ResultSet _rs;

		internal Db2DataReader(Statement statement, ResultSet rs)
		{
			_statement = statement;
			_rs = rs;
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

		public override bool IsClosed
		{
			get { throw new NotImplementedException(); }
		}

		public override int RecordsAffected
		{
			get { throw new NotImplementedException(); }
		}

		public override bool GetBoolean(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override byte GetByte(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
		{
			throw new NotImplementedException();
		}

		public override char GetChar(int ordinal)
		{
			throw new NotImplementedException();
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
			throw new NotImplementedException();
		}

		public override int GetInt32(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override long GetInt64(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override DateTime GetDateTime(int ordinal)
		{
			throw new NotImplementedException();
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

		class RowDescription
		{
			public int FieldCount { get; set; }
			public bool?[] ColumnNullables { get; set; }
			public string[] ColumnNames { get; set; }
			public Type[] ColumnTypes { get; set; }

			public RowDescription(int columnCount)
			{
				FieldCount = columnCount;
				ColumnNullables = new bool?[columnCount];
				ColumnNames = new string[columnCount];
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

					rowDescription.ColumnNullables[column] = IsNullable(meta, ordinal);
					rowDescription.ColumnNames[column] = meta.getColumnName(ordinal);
					rowDescription.ColumnTypes[column] = GetTypeFromDb(meta, ordinal);
				}

				_rowDescription = rowDescription;
			}
		}

		private bool? IsNullable(ResultSetMetaData meta, int ordinal)
		{
			int isNullable = meta.isNullable(ordinal);
			switch (isNullable)
			{
				case 0: // ResultSetMetaData.columnNoNulls
					return false;
				case 1: // ResultSetMetaData.columnNullable
					return true;
				case 2: // ResultSetMetaData.columnNullableUnknown
				default:
					return null;
			}
		}

		private Type GetTypeFromDb(ResultSetMetaData meta, int ordinal)
		{
			var columnType = meta.getColumnTypeName(ordinal);
			switch (columnType)
			{
				case "CLOB":
				default:
					return typeof(String);
			}
		}

		public override object GetValue(int ordinal)
		{
			try
			{
				EnsureTypes();
				var obj = _rs.getObject(ordinal + 1);
				return DbConvert(obj);
			}
			catch (SQLException se)
			{
				throw new Db2Exception(se);
			}
		}

		private object DbConvert(object obj)
		{
			var clob = obj as java.sql.Clob;
			if (clob != null)
			{
				return clob.getSubString(1, (int)clob.length());
			}
			var blob = obj as java.sql.Blob;
			if (blob != null)
			{
				return blob.getBytes(1, (int)blob.length());
			}
			var sqlxml = obj as java.sql.SQLXML;
			if (sqlxml != null)
			{
				var xml = new XmlDocument();
				xml.LoadXml(sqlxml.getString());
				var xmlDeclaration = xml.CreateXmlDeclaration("1.0", "UTF-16", null);
				XmlElement root = xml.DocumentElement;
				xml.InsertBefore(xmlDeclaration, root);
				return xml;
			}

			return obj;
		}

		public override int GetValues(object[] values)
		{
			throw new NotImplementedException();
		}

		public override bool IsDBNull(int ordinal)
		{
			EnsureTypes();
			return _rowDescription.ColumnNullables[ordinal] ?? true;
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

		public override decimal GetDecimal(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override double GetDouble(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override float GetFloat(int ordinal)
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