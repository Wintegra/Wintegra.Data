using System;
using System.Collections.Generic;
using System.Data;

namespace Wintegra.Data.Tests.Db2Client
{
	internal static class Db2Driver
	{
		internal static readonly Dictionary<string, string> Connection = new Dictionary<string, string>()
		{
			{ "odbc", "Driver={IBM DB2 ODBC DRIVER};DataBase=DB1; HostName=192.168.72.135; Protocol=TCPIP;Port=50000;Uid=root;Pwd=password;CurrentSchema=DB01;DB2NETNamedParam=1;HostVarParameters=1" },
			{ "jdbc", "jdbc:db2://192.168.72.135:50000/DB1:currentSchema=DB01;user=root;password=password;fullyMaterializeLobData=true;DB2NETNamedParam=1;" },
		};
		internal const string TypeDb2Driver = "odbc";

		internal static string GetConnectionString(string type = TypeDb2Driver)
		{
			switch (type)
			{
				case "jdbc":
					return Connection["jdbc"];
				case "odbc":
					return Connection["odbc"];
			}
			throw new ArgumentOutOfRangeException("type", type, "Use [odbc|jdbc]");
		}

		internal static IDbConnection GetDbConnection(string type = TypeDb2Driver)
		{
			switch (type)
			{
				case "jdbc":
					return new Wintegra.Data.jdbc.Db2Connection(Connection["jdbc"]);
				case "odbc":
					return new Wintegra.Data.Db2Client.Db2Connection(Connection["odbc"]);
			}
			throw new ArgumentOutOfRangeException("type", type, "Use [odbc|jdbc]");
		}

		internal static IDbConnection GetDbConnectionWithConnectionString(string type = TypeDb2Driver)
		{
			switch (type)
			{
				case "jdbc":
					return new Wintegra.Data.jdbc.Db2Connection();
				case "odbc":
					return new Wintegra.Data.Db2Client.Db2Connection();
			}
			throw new ArgumentOutOfRangeException("type", type, "Use [odbc|jdbc]");
		}
	}
}