﻿using System;
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
						command.CommandText = "INSERT INTO DBG_TABLE(CLOB_TEXT) VALUES(:CLOB_TEXT)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "CLOB_TEXT";
						parameterObject.Value = clob;
						command.Parameters.Add(parameterObject);

						var rowCount =command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT CLOB_TEXT FROM DBG_TABLE";

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
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DB01.DBG_TABLE(DBCLOB_TEXT) VALUES('clob is null')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT DBCLOB_TEXT, CLOB_TEXT FROM DBG_TABLE";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = (String)reader.GetValue(0);
							var actual = (String)reader.GetValue(1);
							Assert.That(msg, Is.EqualTo("clob is null"));
							Assert.That(actual, Is.Null);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

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
						command.CommandText = "INSERT INTO DBG_TABLE(DBCLOB_TEXT) VALUES(:DBCLOB_TEXT)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "DBCLOB_TEXT";
						parameterObject.Value = dbclob;
						command.Parameters.Add(parameterObject);

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT DBCLOB_TEXT FROM DBG_TABLE";

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
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE(CLOB_TEXT) VALUES('dbclob is null')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT CLOB_TEXT, DBCLOB_TEXT FROM DBG_TABLE";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = (String)reader.GetValue(0);
							var actual = (String)reader.GetValue(1);
							Assert.That(msg, Is.EqualTo("dbclob is null"));
							Assert.That(actual, Is.Null);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

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
						command.CommandText = "INSERT INTO DBG_TABLE(BLOB_DATA) VALUES(:BLOB_DATA)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "BLOB_DATA";
						parameterObject.Value = blob;
						command.Parameters.Add(parameterObject);

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT BLOB_DATA FROM DBG_TABLE";

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
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE(CLOB_TEXT) VALUES('blob is null')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT CLOB_TEXT, BLOB_DATA FROM DBG_TABLE";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = (String)reader.GetValue(0);
							var actual = (byte[])reader.GetValue(1);
							Assert.That(msg, Is.EqualTo("blob is null"));
							Assert.That(actual, Is.Null);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

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
						command.CommandText = "INSERT INTO DBG_TABLE(XML_BODY) VALUES(:XML_BODY)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "XML_BODY";
						parameterObject.Value = xml;
						command.Parameters.Add(parameterObject);

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT XML_BODY FROM DBG_TABLE";

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
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE(CLOB_TEXT) VALUES('xml is null')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT CLOB_TEXT, XML_BODY FROM DBG_TABLE";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = (String)reader.GetValue(0);
							XmlDocument actual = (XmlDocument)reader.GetValue(1);

							Assert.That(msg, Is.EqualTo("xml is null"));
							Assert.That(actual, Is.Null);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}
	}
}