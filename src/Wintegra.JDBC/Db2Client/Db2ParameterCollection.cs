using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.Common;
using System.Diagnostics;

namespace Wintegra.JDBC.Db2Client
{
	public sealed class Db2ParameterCollection : DbParameterCollection, IList<Db2Parameter>
	{
		private readonly List<Db2Parameter> _internalList;

		private Dictionary<string, int> _lookup;
		private Dictionary<string, int> _lookupIgnoreCase;


		internal Db2ParameterCollection()
		{
			_internalList = new List<Db2Parameter>();
			InvalidateHashLookups();
		}

		internal void InvalidateHashLookups()
		{
			_lookup = null;
			_lookupIgnoreCase = null;
		}

		#region Db2ParameterCollection Member

		[Browsable(false), DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden)]
		public new Db2Parameter this[string parameterName]
		{
			get
			{
				var index = IndexOf(parameterName);

				if (index == -1)
					throw new ArgumentException("Parameter not found");

				return _internalList[index];
			}
			set
			{
				int index = IndexOf(parameterName);

				if (index == -1)
				{
					throw new ArgumentException("Parameter not found");
				}

				Db2Parameter oldValue = _internalList[index];

				if (value.CleanName != oldValue.CleanName)
				{
					InvalidateHashLookups();
				}

				_internalList[index] = value;
			}
		}

		[Browsable(false), DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden)]
		public Db2Parameter this[int index]
		{
			get { return _internalList[index]; }
			set
			{
				var oldValue = _internalList[index];

				if (oldValue == value)
				{
					// Reasigning the same value is a non-op.
					return;
				}

				if (value.Collection != null)
				{
					throw new InvalidOperationException("The parameter already belongs to a collection");
				}

				if (value.CleanName != oldValue.CleanName)
				{
					InvalidateHashLookups();
				}

				_internalList[index] = value;
				value.Collection = this;
				oldValue.Collection = null;
			}
		}

		public Db2Parameter Add(Db2Parameter value)
		{
			// Do not allow parameters without name.
			if (value.Collection != null)
			{
				throw new InvalidOperationException("The parameter already belongs to a collection");
			}

			_internalList.Add(value);
			value.Collection = this;
			InvalidateHashLookups();

			// Check if there is a name. If not, add a name based of the index+1 of the parameter.
			if (value.ParameterName.Trim() == string.Empty || (value.ParameterName.Length == 1 && value.ParameterName[0] == ':'))
			{
				value.ParameterName = ":" + "Parameter" + _internalList.Count;
				value.AutoAssignedName = true;
			}

			return value;
		}

		void ICollection<Db2Parameter>.Add(Db2Parameter item)
		{
			Add(item);
		}

		public Db2Parameter AddWithValue(string parameterName, object value)
		{
			return Add(new Db2Parameter(parameterName, value));
		}

		#endregion

		#region IDataParameterCollection Member

		public override void RemoveAt(string parameterName)
		{
			if (parameterName == null)
				throw new ArgumentNullException("parameterName");

			RemoveAt(IndexOf(parameterName));
		}

		public override bool Contains(object value)
		{
			if (!(value is Db2Parameter))
			{
				return false;
			}
			return _internalList.Contains((Db2Parameter)value);
		}

		public override bool Contains(string parameterName)
		{
			if (parameterName == null)
				throw new ArgumentNullException("parameterName");

			return (IndexOf(parameterName) != -1);
		}

		public override int IndexOf(string parameterName)
		{
			if (parameterName == null)
			{
				return -1;
			}

			int retIndex;
			int scanIndex;

			if (parameterName.Length > 0 && ((parameterName[0] == ':') || (parameterName[0] == '@')))
			{
				parameterName = parameterName.Remove(0, 1);
			}

			// Using a dictionary is much faster for 5 or more items
			if (_internalList.Count >= 5)
			{
				if (_lookup == null)
				{
					_lookup = new Dictionary<string, int>();
					for (scanIndex = 0; scanIndex < _internalList.Count; scanIndex++)
					{
						var item = _internalList[scanIndex];

						// Store only the first of each distinct value
						if (!_lookup.ContainsKey(item.CleanName))
						{
							_lookup.Add(item.CleanName, scanIndex);
						}
					}
				}

				// Try to access the case sensitive parameter name first
				if (_lookup.TryGetValue(parameterName, out retIndex))
				{
					return retIndex;
				}

				// Case sensitive lookup failed, generate a case insensitive lookup
				if (_lookupIgnoreCase == null)
				{
					_lookupIgnoreCase = new Dictionary<string, int>(Db2Util.InvariantCaseIgnoringStringComparer);
					for (scanIndex = 0; scanIndex < _internalList.Count; scanIndex++)
					{
						var item = _internalList[scanIndex];

						// Store only the first of each distinct value
						if (!_lookupIgnoreCase.ContainsKey(item.CleanName))
						{
							_lookupIgnoreCase.Add(item.CleanName, scanIndex);
						}
					}
				}

				// Then try to access the case insensitive parameter name
				if (_lookupIgnoreCase.TryGetValue(parameterName, out retIndex))
				{
					return retIndex;
				}

				return -1;
			}

			retIndex = -1;

			// Scan until a case insensitive match is found, and save its index for possible return.
			// Items that don't match loosely cannot possibly match exactly.
			for (scanIndex = 0; scanIndex < _internalList.Count; scanIndex++)
			{
				var item = _internalList[scanIndex];
				var comparer = Db2Util.InvariantCaseIgnoringStringComparer;
				if (comparer.Compare(parameterName, item.CleanName) == 0)
				{
					retIndex = scanIndex;

					break;
				}
			}

			// Then continue the scan until a case sensitive match is found, and return it.
			// If a case insensitive match was found, it will be re-checked for an exact match.
			for (; scanIndex < _internalList.Count; scanIndex++)
			{
				var item = _internalList[scanIndex];

				if (item.CleanName == parameterName)
				{
					return scanIndex;
				}
			}

			// If a case insensitive match was found, it will be returned here.
			return retIndex;
		}

		public override void AddRange(Array values)
		{
			foreach (Db2Parameter parameter in values)
				Add(parameter);
		}

		public override void CopyTo(Array array, int index)
		{
			if (array == null)
				throw new ArgumentNullException("array");

			(_internalList as ICollection).CopyTo(array, index);
		}

		protected override DbParameter GetParameter(string parameterName)
		{
			if (parameterName == null)
				throw new ArgumentNullException("parameterName");

			return this[parameterName];
		}

		protected override DbParameter GetParameter(int index)
		{
			return this[index];
		}

		protected override void SetParameter(string parameterName, DbParameter value)
		{
			if (parameterName == null) throw new ArgumentNullException("parameterName");

			this[parameterName] = (Db2Parameter)value;
		}

		protected override void SetParameter(int index, DbParameter value)
		{
			this[index] = (Db2Parameter)value;
		}

		private static void CheckType(object o)
		{
			if (!(o is Db2Parameter))
				throw new InvalidCastException(string.Format("Can't cast {0} into NpgsqlParameter", o.GetType()));
		}

		#endregion

		#region IList Member

		public override bool IsReadOnly { get { return false; } }

		public override void RemoveAt(int index)
		{
			if (_internalList.Count - 1 < index)
			{
				throw new IndexOutOfRangeException();
			}
			Remove(_internalList[index]);
		}

		public override void Insert(int index, object oValue)
		{
			if (oValue == null)
				throw new ArgumentNullException("oValue");

			CheckType(oValue);
			Db2Parameter value = oValue as Db2Parameter;
			Debug.Assert(value != null);
			if (value.Collection != null)
			{
				throw new InvalidOperationException("The parameter already belongs to a collection");
			}

			value.Collection = this;
			_internalList.Insert(index, value);
			InvalidateHashLookups();
		}

		public int IndexOf(Db2Parameter item)
		{
			return _internalList.IndexOf(item);
		}

		public void Insert(int index, Db2Parameter item)
		{
			if (item.Collection != null)
				throw new Exception("The parameter already belongs to a collection");

			_internalList.Insert(index, item);
			item.Collection = this;
			InvalidateHashLookups();
		}

		public void Remove(string parameterName)
		{
			var index = IndexOf(parameterName);
			if (index < 0)
			{
				throw new InvalidOperationException("No parameter with the specified name exists in the collection");
			}
			RemoveAt(index);
		}

		public override void Remove(object oValue)
		{
			if (oValue == null)
				throw new ArgumentNullException("oValue");

			CheckType(oValue);
			var p = oValue as Db2Parameter;
			Debug.Assert(p != null);
			Remove(p);
		}

		public bool TryGetValue(string parameterName, out Db2Parameter parameter)
		{
			int index = IndexOf(parameterName);

			if (index != -1)
			{
				parameter = _internalList[index];

				return true;
			}
			else
			{
				parameter = null;

				return false;
			}
		}

		public override void Clear()
		{
			foreach (var toRemove in _internalList)
			{
				// clean up the parameter so it can be added to another command if required.
				toRemove.Collection = null;
			}
			_internalList.Clear();
			InvalidateHashLookups();
		}

		public override int IndexOf(object value)
		{
			if (value == null)
				throw new ArgumentNullException("value");

			CheckType(value);
			return _internalList.IndexOf((Db2Parameter)value);
		}

		public override int Add(object value)
		{
			CheckType(value);
			Add((Db2Parameter)value);
			return Count - 1;
		}


		public override bool IsFixedSize { get { return  false; } }


		#endregion

		#region ICollection Member

		public override bool IsSynchronized { get { return (_internalList as ICollection).IsSynchronized; } }

		

		[Browsable(false), DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden)]
		public override int Count { get { return _internalList.Count; } }

		public void CopyTo(Db2Parameter[] array, int arrayIndex)
		{
			_internalList.CopyTo(array, arrayIndex);
		}

		bool ICollection<Db2Parameter>.IsReadOnly { get { return false; } }

		public override object SyncRoot { get { return (_internalList as ICollection).SyncRoot; } }

		public bool Contains(Db2Parameter item)
		{
			return _internalList.Contains(item);
		}

		public bool Remove(Db2Parameter item)
		{
			if (item == null)
				throw new ArgumentNullException("item");
			if (item.Collection != this)
				throw new InvalidOperationException("The item does not belong to this collection");

			if (_internalList.Remove(item))
			{
				item.Collection = null;
				InvalidateHashLookups();
				return true;
			}
			return false;
		}

		#endregion

		#region IEnumerable Member

		IEnumerator<Db2Parameter> IEnumerable<Db2Parameter>.GetEnumerator()
		{
			return _internalList.GetEnumerator();
		}

		public override IEnumerator GetEnumerator()
		{
			return _internalList.GetEnumerator();
		}

		#endregion
	}
}