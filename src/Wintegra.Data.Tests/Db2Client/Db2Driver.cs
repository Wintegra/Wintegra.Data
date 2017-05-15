using System;
using System.Collections.Generic;
using System.Data;

namespace Wintegra.Data.Tests.Db2Client
{
	internal static class Db2Driver
	{
		internal static readonly Dictionary<string, string> Connection = new Dictionary<string, string>()
		{
			{ "odbc", "Driver={IBM DB2 ODBC DRIVER};DataBase=DB1; HostName=127.0.0.1; Protocol=TCPIP;Port=50000;Uid=root;Pwd=password;CurrentSchema=DB01;DB2NETNamedParam=1;HostVarParameters=1;OdbcCommandTimeout=37" },
		};
		internal const string TypeDb2Driver = "odbc";

		internal static string GetConnectionString(string type = TypeDb2Driver)
		{
			switch (type)
			{
				case "odbc":
					return Connection["odbc"];
			}
			throw new ArgumentOutOfRangeException("type", type, "Use [odbc]");
		}

		internal static IDbConnection GetDbConnection(string type = TypeDb2Driver)
		{
			switch (type)
			{
				case "odbc":
					return new Wintegra.Data.Db2Client.Db2Connection(Connection["odbc"]);
			}
			throw new ArgumentOutOfRangeException("type", type, "Use [odbc]");
		}

		internal static IDbConnection GetDbConnectionWithConnectionString(string type = TypeDb2Driver)
		{
			switch (type)
			{
				case "odbc":
					return new Wintegra.Data.Db2Client.Db2Connection();
			}
			throw new ArgumentOutOfRangeException("type", type, "Use [odbc]");
		}
	}
}