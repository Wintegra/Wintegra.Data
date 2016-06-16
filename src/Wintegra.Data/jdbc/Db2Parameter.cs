using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;

namespace Wintegra.Data.jdbc
{
	public sealed class Db2Parameter : DbParameter, ICloneable
	{
		private byte _precision;
		private byte _scale;
		private int _size;

		Db2DataType? _db2DataType;
		DbType? _dbType;
		private string _name;
		private object _value;


		private Db2ParameterCollection _collection;


		internal bool AutoAssignedName;


		internal Db2Parameter()
		{
			SourceColumn = String.Empty;
			Direction = ParameterDirection.Input;
			SourceVersion = DataRowVersion.Current;
		}

		internal Db2Parameter(String parameterName, object value)
			: this()
		{
			ParameterName = parameterName;
			Value = value;
		}

		[Category("Data"), TypeConverter(typeof(StringConverter))]
		public override object Value
		{
			get { return _value; }
			set { _value = value; }
		}

		public override bool IsNullable { get; set; }

		[Category("Data"), DefaultValue(ParameterDirection.Input)]
		public override ParameterDirection Direction { get; set; }

		[Category("Data"), DefaultValue((Byte)0)]
#pragma warning disable CS0114
		public byte Precision
#pragma warning restore CS0114
		{
			get { return _precision; }
			set
			{
				_precision = value;
			}
		}

		[Category("Data"), DefaultValue((Byte)0)]
#pragma warning disable CS0114
		public byte Scale
#pragma warning restore CS0114
		{
			get { return _scale; }
			set
			{
				_scale = value;
			}
		}

		[Category("Data"), DefaultValue(0)]
		public override int Size
		{
			get { return _size; }
			set
			{
				if (value < -1)
					throw new ArgumentException(string.Format("Invalid parameter Size value '{0}'. The value must be greater than or equal to 0.", value));

				_size = value;
			}
		}

		[RefreshProperties(RefreshProperties.All)]
		[Category("Data"), DefaultValue(DbType.Object)]
		public override DbType DbType
		{
			get
			{
				if (_dbType.HasValue) {
					return _dbType.Value;
				}

				if (_value != null) {
					return TypeMap.ToDbType(_value.GetType());
				}
				
				return DbType.Object;
			}
			set
			{
				if (value == DbType.Object) 
				{
					_dbType = null;
					_db2DataType = null;
				}
				else 
				{
					_dbType = value;
					_db2DataType = TypeMap.FromDbType(value);
				}
			}
		}

		[RefreshProperties(RefreshProperties.All)]
		[Category("Data"), DefaultValue(Db2DataType.Unknown)]
		public Db2DataType Db2DataType
		{
			get
			{
				if (_db2DataType.HasValue)
				{
					return _db2DataType.Value;
				}

				if (_value != null)
				{
					return TypeMap.FromDbType(_value);
				}

				return Db2DataType.Unknown;
			}
			set
			{
				_db2DataType = value;
				_dbType = TypeMap.ToDbType(value);
			}
		}

		[DefaultValue("")]
		public override string ParameterName
		{
			get { return _name; }
			set
			{
				_name = value;
				if (value == null)
				{
					_name = String.Empty;
				}

				_name = _name.Trim();

				if (_collection != null)
				{
					_collection.InvalidateHashLookups();
				}
				AutoAssignedName = false;
			}
		}

		[Category("Data"), DefaultValue("")]
		public override string SourceColumn { get; set; }

		[Category("Data"), DefaultValue(DataRowVersion.Current)]
		public override DataRowVersion SourceVersion { get; set; }

		public override bool SourceColumnNullMapping { get; set; }

		public override void ResetDbType()
		{
			_dbType = null;
			_db2DataType = null;
			Value = Value;
		}

		public object Clone()
		{
			var clone = new Db2Parameter
			{
				_precision = _precision,
				_scale = _scale,
				_size = _size,
				_dbType = _dbType,
				_db2DataType = _db2DataType,
				Direction = Direction,
				IsNullable = IsNullable,
				_name = _name,
				SourceColumn = SourceColumn,
				SourceVersion = SourceVersion,
				_value = _value,
				SourceColumnNullMapping = SourceColumnNullMapping,
				AutoAssignedName = AutoAssignedName
			};
			return clone;
		}



		public Db2ParameterCollection Collection
		{
			get { return _collection; }
			internal set
			{
				_collection = value;
			}
		}

		internal string CleanName
		{
			get
			{
				string name = ParameterName;
				if (name.Length > 0 && (name[0] == ':' || name[0] == '@'))
				{
					return name.Substring(1);
				}
				return name;
			}
		}
	}
}