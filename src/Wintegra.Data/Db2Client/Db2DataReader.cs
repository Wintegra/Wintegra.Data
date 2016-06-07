using System;
using System.Data;
using System.IO;
using System.Text;
using System.Xml;

namespace Wintegra.Data.Db2Client
{
	internal class Db2DataReader: IDataReader
	{
		private static byte[] GetBlob(IDataReader dr, int i)
		{
			long dataIndex = 0;
			const int length = 4096;
			var buffer = new byte[length];
			using (var ms = new MemoryStream())
			{
				long count;
				do
				{
					try
					{
						count = dr.GetBytes(i, dataIndex, buffer, 0, length);
					}
					catch (InvalidCastException ex)
					{
						if (ex.Message.Contains("DBNull") || ex.Message.Equals("Specified cast is not valid."))
						{
							return null;
						}
						throw;
					}
					ms.Write(buffer, 0, (int)count);
					dataIndex += count;
				} while (count == length);

				ms.Position = 0;
				return ms.ToArray();
			}
		}

		private static void LoadString(XmlDocument xmlDocument, string xmlString, Encoding encoding = null)
		{
			if (encoding == null)
			{
				encoding = Encoding.UTF8;
			}

			byte[] encodedString = encoding.GetBytes(xmlString);
			using (var ms = new MemoryStream(encodedString))
			{
				ms.Flush();
				ms.Position = 0;
				xmlDocument.Load(ms);
			}
		}


		enum ColumnTypeEnum
		{
			Default = 0,
			Blob = 1,
			Clob = 2,
			Xml = 3,
		}

		private readonly IDataReader _reader;

		public Db2DataReader(IDataReader reader)
		{
			this._reader = reader;
		}

		public void Dispose()
		{
			_reader.Dispose();
		}

		public string GetName(int i)
		{
			return _reader.GetName(i);
		}

		public string GetDataTypeName(int i)
		{
			return _reader.GetDataTypeName(i);
		}

		private readonly object _syncRoot = new object();
		private Type[] _columnTypes = null;
		private ColumnTypeEnum[] _column = null;
		private string[] _columnNames = null;

		private void EnsureTypes()
		{
			if (_columnTypes != null) return;
			lock (_syncRoot)
			{
				if (_columnTypes != null) return;

				_columnTypes = new Type[FieldCount];
				_column = new ColumnTypeEnum[FieldCount];
				_columnNames = new string[FieldCount];

				for (var i = 0; i < FieldCount; ++i)
				{
					_columnNames[i] = GetName(i);
					var typeName = GetDataTypeName(i);
					switch (typeName)
					{
						case "CLOB":
						case "DBCLOB":
							_column[i] = ColumnTypeEnum.Clob;
							_columnTypes[i] = typeof (String);
							break;
						case "BLOB":
							_column[i] = ColumnTypeEnum.Blob;
							_columnTypes[i] = typeof (byte[]);
							break;
						case "XML":
							_column[i] = ColumnTypeEnum.Xml;
							_columnTypes[i] = typeof (XmlDocument);
							break;
						default:
							_column[i] = ColumnTypeEnum.Default;
							_columnTypes[i] = _reader.GetFieldType(i);
							break;
					}
				}

			}

		}

		public Type GetFieldType(int i)
		{
			EnsureTypes();
			return _columnTypes[i];
		}

		public object GetValue(int i)
		{
			EnsureTypes();
			switch (_column[i])
			{
				case ColumnTypeEnum.Blob:
					return Db2DataReader.GetBlob(this, i);
				case ColumnTypeEnum.Clob:
					return GetString(i);
				case ColumnTypeEnum.Xml:
					return GetXmlDocument(i);
				default:
					return _reader.GetValue(i);
			}
		}

		public int GetValues(object[] values)
		{
			return _reader.GetValues(values);
		}

		public int GetOrdinal(string name)
		{
			return _reader.GetOrdinal(name);
		}

		public bool GetBoolean(int i)
		{
			return _reader.GetBoolean(i);
		}

		public byte GetByte(int i)
		{
			return _reader.GetByte(i);
		}

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			return _reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		}

		public char GetChar(int i)
		{
			return _reader.GetChar(i);
		}

		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			return _reader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		}

		public Guid GetGuid(int i)
		{
			return _reader.GetGuid(i);
		}

		public short GetInt16(int i)
		{
			return _reader.GetInt16(i);
		}

		public int GetInt32(int i)
		{
			return _reader.GetInt32(i);
		}

		public long GetInt64(int i)
		{
			return _reader.GetInt64(i);
		}

		public float GetFloat(int i)
		{
			return _reader.GetFloat(i);
		}

		public double GetDouble(int i)
		{
			return _reader.GetDouble(i);
		}

		public string GetString(int i)
		{
			try
			{
				return _reader.GetString(i);
			}
			catch (InvalidCastException ex)
			{
				if (ex.Message.Contains("DBNull"))
				{
					return null;
				}
				throw;
			}
		}

		public decimal GetDecimal(int i)
		{
			return _reader.GetDecimal(i);
		}

		public DateTime GetDateTime(int i)
		{
			return _reader.GetDateTime(i);
		}

		public IDataReader GetData(int i)
		{
			return _reader.GetData(i);
		}

		public XmlDocument GetXmlDocument(int i)
		{
			var xmlBody = GetString(i);
			if (string.IsNullOrEmpty(xmlBody)) return null;
			var doc = new XmlDocument();
			Db2DataReader.LoadString(doc, xmlBody, Encoding.Unicode);
			return doc;
		}

		public bool IsDBNull(int i)
		{
			return _reader.IsDBNull(i);
		}

		public int FieldCount
		{
			get { return _reader.FieldCount; }
		}

		public object this[int i]
		{
			get { return GetValue(i); }
		}

		private int _nextNameNo = 0;
		public object this[string name]
		{
			get
			{
				EnsureTypes();
				var n = name.ToUpper();
				int? j = null;
				for (int p = 0; p < FieldCount; p++)
				{
					int i = (_nextNameNo + p)%FieldCount;
					if (_columnNames[i].Equals(n))
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
				return this[j.Value];
			}
		}

		public void Close()
		{
			_reader.Close();
		}

		public DataTable GetSchemaTable()
		{
			return _reader.GetSchemaTable();
		}

		public bool NextResult()
		{
			_columnTypes = null;
			_column = null;
			return _reader.NextResult();
		}

		public bool Read()
		{
			return _reader.Read();
		}

		public int Depth
		{
			get { return _reader.Depth; }
		}

		public bool IsClosed
		{
			get { return _reader.IsClosed; }
		}

		public int RecordsAffected
		{
			get { return _reader.RecordsAffected; }
		}
	}
}