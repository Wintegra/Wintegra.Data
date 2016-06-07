using System;
using System.Collections;
using System.Data;
using System.IO;
using System.Xml;

namespace Wintegra.Data.Db2Client
{
	internal class Db2DataParameterCollection : IDataParameterCollection
	{
		private static byte[] ToArrayBytes(XmlDocument document)
		{
			using (var ms = new MemoryStream())
			{
				document.Save(ms);
				return ms.ToArray();
			}
		}

		private readonly IDataParameterCollection _parameters;

		public Db2DataParameterCollection(IDataParameterCollection parameters)
		{
			_parameters = parameters;
		}

		public IEnumerator GetEnumerator()
		{
			return _parameters.GetEnumerator();
		}

		public void CopyTo(Array array, int index)
		{
			_parameters.CopyTo(array, index);
		}

		public int Count
		{
			get { return _parameters.Count; }
		}

		public object SyncRoot
		{
			get { return _parameters.SyncRoot; }
		}

		public bool IsSynchronized
		{
			get { return _parameters.IsSynchronized; }
		}

		public int Add(object value)
		{
			object parameter = value;
			if (parameter is IDbDataParameter)
			{
				object obj = ((IDbDataParameter) parameter).Value;
				if (obj != null)
				{
					var type = obj.GetType();
					if (type == typeof (XmlDocument))
					{
						((IDbDataParameter)parameter).Value = Db2DataParameterCollection.ToArrayBytes((XmlDocument)obj);
					}
					else if (type == typeof(XmlDeclaration))
					{
						return -1; // skip for Dapper
					}
					else if (type == typeof(XmlElement))
					{
						((IDbDataParameter)parameter).Value = Db2DataParameterCollection.ToArrayBytes(((XmlElement)obj).OwnerDocument); // compatible with Dapper
					}
				}
			}
			return _parameters.Add(parameter);
		}

		public bool Contains(object value)
		{
			return _parameters.Contains(value);
		}

		public void Clear()
		{
			_parameters.Clear();
		}

		public int IndexOf(object value)
		{
			return _parameters.IndexOf(value);
		}

		public void Insert(int index, object value)
		{
			_parameters.Insert(index, value);
		}

		public void Remove(object value)
		{
			_parameters.Remove(value);
		}

		public void RemoveAt(int index)
		{
			_parameters.RemoveAt(index);
		}

		object IList.this[int index]
		{
			get { return _parameters[index]; }
			set { _parameters[index] = value; }
		}

		public bool IsReadOnly
		{
			get { return _parameters.IsReadOnly; }
		}

		public bool IsFixedSize
		{
			get { return _parameters.IsFixedSize; }
		}

		public bool Contains(string parameterName)
		{
			return _parameters.Contains(parameterName);
		}

		public int IndexOf(string parameterName)
		{
			return _parameters.IndexOf(parameterName);
		}

		public void RemoveAt(string parameterName)
		{
			_parameters.RemoveAt(parameterName);
		}

		object IDataParameterCollection.this[string parameterName]
		{
			get { return _parameters[parameterName]; }
			set { _parameters[parameterName] = value; }
		}
	}
}