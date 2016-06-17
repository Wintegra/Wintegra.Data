using System;
using System.Data;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Xml;
using NUnit.Framework;

namespace Wintegra.Data.Tests.Db2Client
{
	[TestFixture]
	public class Db2DataReaderTest
	{
		[SetUp]
		public void SetUp()
		{
			Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
			Thread.CurrentThread.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");
		}

		#region String data types

		// CHARACTER
		// VARCHAR

		#region CLOB
		[Test]
		public void TestWriteAndReadClob(
			[Values("odbc", "jdbc")] string type,
			[Values(1024, 4096, 8192, 65536, 1048576, 4194304)] int length)
		{
			string clob = Utility.RandomString(length);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_CLOB(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = clob;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_CLOB";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							Assert.That(reader.GetValue(0), Is.EqualTo(clob));
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

		[Test]
		public void TestReadNullClob([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_CLOB(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_CLOB";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var actual = (String)reader.GetValue(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(actual, Is.Null);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

		#endregion

		// GRAPHIC
		// VARGRAPHIC

		#region DBCLOB

		[Test]
		public void TestWriteAndReadDbclob(
			[Values("odbc", "jdbc")] string type,
			[Values(1024, 4096, 8192, 65536, 1048576, 4194304)] int length)
		{
			string dbclob = Utility.RandomString(length);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DBCLOB(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = dbclob;
						command.Parameters.Add(parameterObject);

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_DBCLOB";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							Assert.That(reader.GetValue(0), Is.EqualTo(dbclob));
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

		[Test]
		public void TestReadNullDbclob([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DBCLOB(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_DBCLOB";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var actual = (String)reader.GetValue(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(actual, Is.Null);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

		#endregion

		#region BLOB

		[Test]
		public void TestWriteAndReadBlob(
			[Values("odbc", "jdbc")] string type,
			[Values(1024, 4096, 8192, 65536, 1048576, 4194304)] int length)
		{
			byte[] blob = Encoding.UTF8.GetBytes(Utility.RandomString(length));
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_BLOB(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = blob;
						command.Parameters.Add(parameterObject);

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_BLOB";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							Assert.That(reader.GetValue(0), Is.EqualTo(blob));
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

		[Test]
		public void TestReadNullBlob([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_BLOB(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_BLOB";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var actual = (byte[])reader.GetValue(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(actual, Is.Null);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

		#endregion

		#endregion
		#region Numeric data types

		// SMALLINT
		// INTEGER 
		// BIGINT
		// DECIMAL 
		// DECFLOAT
		// REAL
		// DOUBLE

		#endregion

		#region Date, time, and timestamp data types

		// DATE
		// TIME
		// TIMESTAMP

		#endregion 

		#region XML data type

		[Test]
		public void TestWriteAndReadXml(
			[Values("odbc", "jdbc")] string type,
			[Values(1024, 4096, 8192, 65536, 1048576, 4194304)] int length)
		{
			var d = new XmlObjectData()
			{
				Field = Utility.RandomString(length),
			};
			var xml = d.ToXmlDocument();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_XML(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = xml;
						command.Parameters.Add(parameterObject);

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_XML";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							XmlDocument actual = (XmlDocument)reader.GetValue(0);

							Assert.That(actual.OuterXml
								.Substring("<?xml version=\"1.0\" encoding=\"UTF-16\"?>".Length),
								Is.EqualTo(xml.OuterXml
								.Substring("<?xml version=\"1.0\"?>".Length)));
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

		[Test]
		public void TestReadNullXml([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_XML(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_XML";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							XmlDocument actual = (XmlDocument)reader.GetValue(1);

							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(actual, Is.Null);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}


		#endregion
	}
}