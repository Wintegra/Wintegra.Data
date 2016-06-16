using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using NUnit.Framework;

namespace Wintegra.Data.Tests.Db2Client
{
	[TestFixture]
	public class Db2ConnectionTest
	{
		[SetUp]
		public void SetUp()
		{
			Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
			Thread.CurrentThread.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");
		}

		[Test]
		public void BasicLifecycle(
			[Values("jdbc")] string type)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				var conn = db as DbConnection;
				Assert.That(conn, Is.Not.Null);

				bool eventOpen = false, eventClosed = false;
				conn.StateChange += (s, e) =>
				{
					if (e.OriginalState == ConnectionState.Closed && e.CurrentState == ConnectionState.Open)
						eventOpen = true;
					if (e.OriginalState == ConnectionState.Open && e.CurrentState == ConnectionState.Closed)
						eventClosed = true;
				};

				Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));

				conn.Open();
				Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
				Assert.That(eventOpen, Is.True);


				conn.Close();
				Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
				Assert.That(eventClosed, Is.True);

				eventOpen = eventClosed = false;
				conn.Open();
				Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
			}
		}


		[Test]
		public void TestSelectAndRead(
			[Values("odbc", "jdbc")] string type,
			[Values("It is work", "Any simple string")] string name)
		{
			string d;
			using (var db = Db2Driver.GetDbConnection(type))
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
			[Values("odbc", "jdbc")] string type,
			[Values("It is work", "Any simple string")] string name)
		{
			string d;
			using (var db = Db2Driver.GetDbConnectionWithConnectionString(type))
			{
				db.ConnectionString = Db2Driver.GetConnectionString(type);
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
			[Values("odbc", "jdbc")] string type,
			[Values("It is work", "Any simple string")] string name,
			[Values(":ID", ":1")] string parameterName)
		{
			string d;
			using (var db = Db2Driver.GetDbConnection(type))
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