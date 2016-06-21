using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.Common;
using System.Diagnostics;
using System.Reflection;

namespace Wintegra.Data.jdbc
{
	public sealed class Db2ConnectionStringBuilder : DbConnectionStringBuilder, IDictionary<string, object>
	{
		private string _connectionString = "";

		#region Fields

		private static readonly Dictionary<string, PropertyInfo> PropertiesByKeyword;

		private static readonly Dictionary<string, string> PropertyNameToCanonicalKeyword;

		private static readonly Dictionary<PropertyInfo, object> PropertyDefaults;

		private static readonly string[] Empty = new string[0];

		#endregion

		#region Constructors

		public Db2ConnectionStringBuilder(string connectionString)
		{
			Init();
			ConnectionString = connectionString;
		}

		private void Init()
		{
			foreach (var kv in PropertyDefaults)
			{
				kv.Key.SetValue(this, kv.Value, (object[])null);
				base.Clear();
			}
		}

		#endregion

		#region Static initialization

		static Db2ConnectionStringBuilder()
		{
			var properties = new List<PropertyInfo>();
			foreach (var p in typeof(Db2ConnectionStringBuilder).GetProperties())
			{
				var attributes = p.GetCustomAttributes(typeof(Db2ConnectionStringPropertyAttribute), true);
				if (attributes.Length == 0) continue;
				properties.Add(p);
			}

			var propertiesByKeyword = new Dictionary<string, PropertyInfo>();
			var propertyNameToCanonicalKeyword = new Dictionary<string, string>();
			var propertyDefaults = new Dictionary<PropertyInfo, object>();

			foreach (var p in properties)
			{
				var attributes = p.GetCustomAttributes(typeof(Db2ConnectionStringPropertyAttribute), true);
				var propertyAttribute = (Db2ConnectionStringPropertyAttribute)attributes[0];

				attributes = p.GetCustomAttributes(typeof(DisplayNameAttribute), true);
				var displayNameAttribute = (DisplayNameAttribute) attributes[0];
				
				
				var propertyName = p.Name.ToUpperInvariant();
				var displayName = displayNameAttribute.DisplayName.ToUpperInvariant();

				var k = new List<string>();
				k.Add(displayName);
				if (propertyName != displayName) k.Add(propertyName);
				foreach (var a in propertyAttribute.Aliases)
				{
					k.Add(a.ToUpperInvariant());
				}

				foreach (var keyword in k)
				{
					propertiesByKeyword.Add(keyword, p);
				}

				propertyNameToCanonicalKeyword.Add(p.Name, displayNameAttribute.DisplayName);

				attributes = p.GetCustomAttributes(typeof(ObsoleteAttribute), true);
				if (attributes.Length != 0) continue;

				attributes = p.GetCustomAttributes(typeof(DefaultValueAttribute), true);
				if (attributes.Length > 0)
				{
					var defaultValueAttribute = (DefaultValueAttribute) attributes[0];
					propertyDefaults.Add(p, defaultValueAttribute.Value);
				}
				else
				{
					if (p.PropertyType.IsValueType)
					{
						propertyDefaults.Add(p, Activator.CreateInstance(p.PropertyType));
					}
					else
					{
						propertyDefaults.Add(p, null);
					}
				}

			}

			PropertiesByKeyword = propertiesByKeyword;
			PropertyNameToCanonicalKeyword = propertyNameToCanonicalKeyword;
			PropertyDefaults = propertyDefaults;
		}

		#endregion

		#region Non-static property handling

		public override object this[string keyword]
		{
			get
			{
				object value;
				if (!TryGetValue(keyword, out value))
				{
					throw new ArgumentException("Keyword not supported: " + keyword, "keyword");
				}
				return value;
			}
			set
			{
				if (value == null)
				{
					Remove(keyword);
					return;
				}

				var p = GetProperty(keyword);
				try
				{
					object convertedValue;
					if (p.PropertyType.IsEnum && value is string)
					{
						convertedValue = Enum.Parse(p.PropertyType, (string)value);
					}
					else
					{
						convertedValue = Convert.ChangeType(value, p.PropertyType);
					}
					p.SetValue(this, convertedValue, (object[])null);
				}
				catch (Exception e)
				{
					throw new ArgumentException("Couldn't set " + keyword, keyword, e);
				}
			}
		}

		public void Add(KeyValuePair<string, object> item)
		{
			this[item.Key] = item.Value;
		}

		public override bool Remove(string keyword)
		{
			var p = GetProperty(keyword);
			string cannonicalName = PropertyNameToCanonicalKeyword[p.Name];
			var removed = base.ContainsKey(cannonicalName);
			p.SetValue(this, PropertyDefaults[p], (object[])null);
			base.Remove(cannonicalName);
			return removed;
		}

		public bool Remove(KeyValuePair<string, object> item)
		{
			return Remove(item.Key);
		}

		public override void Clear()
		{
			Debug.Assert(Keys != null);
			var keys = new string[Keys.Count];
			Keys.CopyTo(keys, 0);

			foreach (var k in keys)
			{
				Remove(k);
			}
		}

		public override bool ContainsKey(string keyword)
		{
			if (keyword == null)
				throw new ArgumentNullException("keyword");

			return PropertiesByKeyword.ContainsKey(keyword.ToUpperInvariant());
		}

		public bool Contains(KeyValuePair<string, object> item)
		{
			object value;
			return TryGetValue(item.Key, out value) &&
				((value == null && item.Value == null) || (value != null && value.Equals(item.Value)));
		}

		private static PropertyInfo GetProperty(string keyword)
		{
			PropertyInfo p;
			if (!PropertiesByKeyword.TryGetValue(keyword.ToUpperInvariant(), out p))
			{
				throw new ArgumentException("Keyword not supported: " + keyword, "keyword");
			}
			return p;
		}

		public override bool TryGetValue(string keyword, out object value)
		{
			if (keyword == null)
				throw new ArgumentNullException("keyword");

			PropertyInfo p;
			if (!PropertiesByKeyword.TryGetValue(keyword.ToUpperInvariant(), out p))
			{
				value = null;
				return false;
			}

			value = GetProperty(keyword).GetValue(this, (object[])null) ?? "";
			return true;
		}

		private void SetValue(string propertyName, object value)
		{
			var canonicalKeyword = PropertyNameToCanonicalKeyword[propertyName];
			if (value == null)
			{
				base.Remove(canonicalKeyword);
			}
			else
			{
				base[canonicalKeyword] = value;
			}
		}

		#endregion

		#region ConnectionString

		public new string ConnectionString
		{
			get { return _connectionString; }
			set
			{
				var connectionOptions = new Db2ConnectionOptions(value);
				string connectionString = this._connectionString;
				this.Clear();
				try
				{
					foreach (var nameValuePair in connectionOptions.KeyChain)
					{
						if (nameValuePair.Value != null)
							this[nameValuePair.Key] = (object)nameValuePair.Value;
						else
							this.Remove(nameValuePair.Key);
					}
					_connectionString = value;
				}
				catch (ArgumentException ex)
				{
					this._connectionString = connectionString;
					throw;
				}
			}
		}

		#endregion

		#region Properties - Connection

		[Category("CLI/ODBC configuration keyword")]
		[Description("The DB2 .NET Data Provider recognizes named parameters as parameters, but ignores positioned parameters, in SQL statements.")]
		[DisplayName("DB2NETNamedParam")]
		[Db2ConnectionStringProperty]
		public bool NamedParameters
		{
			get { return _namedParameters; }
			set
			{
				_namedParameters = value;
				SetValue("NamedParameters", value);
			}
		}
		private bool _namedParameters;

		#endregion



		#region Misc

		internal Db2ConnectionStringBuilder Clone()
		{
			return new Db2ConnectionStringBuilder(ConnectionString);
		}

		public override bool Equals(object obj)
		{
			var o = obj as Db2ConnectionStringBuilder;
			return o != null && o.ConnectionString == ConnectionString;
		}

		public override int GetHashCode()
		{
			return ConnectionString.GetHashCode();
		}

		#endregion

		#region IDictionary<string, object>

		public new ICollection<string> Keys
		{
			get
			{
				var list = new List<string>(base.Keys.Count);
				foreach (var k in base.Keys)
				{
					list.Add((string)k);
				}
				return list;
			}
		}

		public new ICollection<object> Values
		{
			get
			{
				var list = new List<object>(base.Values.Count);
				foreach (var value in base.Values)
				{
					list.Add((object)value);
				}
				return list;
			}
		}

		public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
		{
			foreach (var kv in this)
				array[arrayIndex++] = kv;
		}

		public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
		{
			foreach (var k in Keys)
				yield return new KeyValuePair<string, object>(k, this[k]);
		}

		#endregion

		#region Attributes

		[AttributeUsage(AttributeTargets.Property)]
		class Db2ConnectionStringPropertyAttribute : Attribute
		{
			internal string[] Aliases { get; private set; }

			internal Db2ConnectionStringPropertyAttribute()
			{
				Aliases = Empty;
			}

			internal Db2ConnectionStringPropertyAttribute(params string[] aliases)
			{
				Aliases = aliases;
			}
		}

		#endregion
	}
}