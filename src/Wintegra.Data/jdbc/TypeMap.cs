using System;
using System.Data;

namespace Wintegra.Data.jdbc
{
	internal sealed class TypeMap
	{
		public static DbType ToDbType(Type getType)
		{
			throw new NotImplementedException();
		}

		public static DbType ToDbType(object getType)
		{
			throw new NotImplementedException();
		}

		public static Db2DataType FromDbType(DbType value)
		{
			switch (value)
			{
				case DbType.Binary:
					return Db2DataType.Binary;

				case DbType.Int16:
					return Db2DataType.Int16;
				case DbType.Int32:
					return Db2DataType.Int32;
				case DbType.Int64:
					return Db2DataType.Int64;


				case DbType.AnsiString:
				case DbType.String:
				case DbType.AnsiStringFixedLength:
				case DbType.StringFixedLength:
					return Db2DataType.String;
			}

			throw new ArgumentOutOfRangeException(string.Format("Internal jdbc bug: unexpected value {0} of enum Db2DataType. Please file a bug.", value));
		}

		public static Db2DataType FromDbType(object value)
		{
			return FromDbType(value.GetType());
		}
	}
}