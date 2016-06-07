using System.Data;
using NUnit.Framework;

namespace Wintegra.Data.Tests.Db2Client
{
	[TestFixture]
	public class Db2ConnectionTest
	{
		internal const string ConnectionString = "Driver={IBM DB2 ODBC DRIVER};DataBase=DB1; HostName=192.168.72.135; Protocol=TCPIP;Port=50000;Uid=root;Pwd=password;CurrentSchema=DB01;DB2NETNamedParam=1;HostVarParameters=1";

		[Test]
		public void TestSelectAndRead(
			[Values("It is work", "Any simple string")] string name)
		{
			string d;
			using (var db = new Wintegra.Data.Db2Client.Db2Connection(ConnectionString))
			{
				db.Open();
				using (var command = db.CreateCommand())
				{
					command.CommandText = "SELECT * FROM TABLE(VALUES('"  + name +"')) AS T(LOG)";

					using (var reader = command.ExecuteReader())
					{
						reader.Read();
						d = reader.GetString(0);
					}
				}
			}
			Assert.That(d, Is.EqualTo(name));
		}
		
		[Test]
		public void TestSetConnectionStringAndRead(
			[Values("It is work", "Any simple string")] string name)
		{
			string d;
			using (var db = new Wintegra.Data.Db2Client.Db2Connection())
			{
				db.ConnectionString = ConnectionString;
				db.Open();
				using (var command = db.CreateCommand())
				{
					command.CommandText = "SELECT * FROM TABLE(VALUES('"  + name +"')) AS T(LOG)";

					using (var reader = command.ExecuteReader())
					{
						reader.Read();
						d = reader.GetString(0);
					}
				}
				db.Close();
			}
			Assert.That(d, Is.EqualTo(name));
		}

		[Test]
		public void TestSelectWhereAndRead(
			[Values("It is work", "Any simple string")] string name,
			[Values("@ID", "@1")] string parameterName)
		{
			string d;
			using (var db = new Wintegra.Data.Db2Client.Db2Connection(ConnectionString))
			{
				db.Open();
				using (var command = db.CreateCommand())
				{
					command.CommandText = "SELECT * FROM TABLE(VALUES(1, '" + name + "')) AS T(ID, LOG) WHERE ID = " + parameterName + " AND (0=0)";

					IDbDataParameter parameterObject = command.CreateParameter();
					parameterObject.ParameterName = parameterName;
					parameterObject.Value = "1";
					command.Parameters.Add(parameterObject);

					using (var reader = command.ExecuteReader())
					{
						reader.Read();
						d = reader.GetString(1);
					}
				}
			}
			Assert.That(d, Is.EqualTo(name));
		}
	}
}