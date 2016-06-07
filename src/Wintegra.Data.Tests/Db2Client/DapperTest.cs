using System;
using System.Text;
using System.Xml;
using Dapper;
using NUnit.Framework;

namespace Wintegra.Data.Tests.Db2Client
{
	[TestFixture]
	public class DapperTest
	{
		class DBG_TABLE
		{
			public String CLOB_TEXT { get; set; }
			public String DBCLOB_TEXT { get; set; }
			public byte[] BLOB_DATA { get; set; }
			public XmlDocument XML_BODY { get; set; }
		}

		[Test]
		public void TestWriteAndReadClob(
			[Values(1024, 4096, 8192, 65536, 1048576, 4194304)] int length)
		{
			string clob = Utility.RandomString(length);
			using (var db = new Wintegra.Data.Db2Client.Db2Connection(Db2ConnectionTest.ConnectionString))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE(CLOB_TEXT) VALUES(@CLOB_TEXT)", new { CLOB_TEXT = clob }, tn);
					var list = db.QueryObjects<DBG_TABLE>("SELECT CLOB_TEXT FROM DBG_TABLE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.CLOB_TEXT, Is.Not.Null);
					Assert.That(actual.DBCLOB_TEXT, Is.Null);
					Assert.That(actual.BLOB_DATA, Is.Null);
					Assert.That(actual.XML_BODY, Is.Null);

					Assert.That(actual.CLOB_TEXT, Is.EqualTo(clob));
				}
			}
		}

		[Test]
		public void TestReadNullClob()
		{
			using (var db = new Wintegra.Data.Db2Client.Db2Connection(Db2ConnectionTest.ConnectionString))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE(DBCLOB_TEXT) VALUES('clob is null')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE>("SELECT DBCLOB_TEXT, CLOB_TEXT FROM DBG_TABLE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.CLOB_TEXT, Is.Null);
					Assert.That(actual.DBCLOB_TEXT, Is.EqualTo("clob is null"));
					Assert.That(actual.BLOB_DATA, Is.Null);
					Assert.That(actual.XML_BODY, Is.Null);
				}
			}
		}

		[Test]
		public void TestWriteAndReadDbclob(
			[Values(1024, 4096, 8192, 65536, 1048576, 4194304)] int length)
		{
			string dbclob = Utility.RandomString(length);
			using (var db = new Wintegra.Data.Db2Client.Db2Connection(Db2ConnectionTest.ConnectionString))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE(DBCLOB_TEXT) VALUES(@DBCLOB_TEXT)", new { DBCLOB_TEXT = dbclob }, tn);
					var list = db.QueryObjects<DBG_TABLE>("SELECT DBCLOB_TEXT FROM DBG_TABLE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.CLOB_TEXT, Is.Null);
					Assert.That(actual.DBCLOB_TEXT, Is.Not.Null);
					Assert.That(actual.BLOB_DATA, Is.Null);
					Assert.That(actual.XML_BODY, Is.Null);

					Assert.That(actual.DBCLOB_TEXT, Is.EqualTo(dbclob));
				}
			}
		}

		[Test]
		public void TestReadNullDbclob()
		{
			using (var db = new Wintegra.Data.Db2Client.Db2Connection(Db2ConnectionTest.ConnectionString))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE(CLOB_TEXT) VALUES('dbclob is null')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE>("SELECT CLOB_TEXT, DBCLOB_TEXT FROM DBG_TABLE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.CLOB_TEXT, Is.EqualTo("dbclob is null"));
					Assert.That(actual.DBCLOB_TEXT, Is.Null);
					Assert.That(actual.BLOB_DATA, Is.Null);
					Assert.That(actual.XML_BODY, Is.Null);
				}
			}
		}

		[Test]
		public void TestWriteAndReadBlob(
			[Values(1024, 4096, 8192, 65536, 1048576, 4194304)] int length)
		{
			byte[] blob = Encoding.UTF8.GetBytes(Utility.RandomString(length));
			using (var db = new Wintegra.Data.Db2Client.Db2Connection(Db2ConnectionTest.ConnectionString))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE(BLOB_DATA) VALUES(@BLOB_DATA)", new { BLOB_DATA = blob }, tn);
					var list = db.QueryObjects<DBG_TABLE>("SELECT BLOB_DATA FROM DBG_TABLE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.CLOB_TEXT, Is.Null);
					Assert.That(actual.DBCLOB_TEXT, Is.Null);
					Assert.That(actual.BLOB_DATA, Is.Not.Null);
					Assert.That(actual.XML_BODY, Is.Null);

					Assert.That(actual.BLOB_DATA, Is.EqualTo(blob));
				}
			}
		}

		[Test]
		public void TestReadNullBlob()
		{
			using (var db = new Wintegra.Data.Db2Client.Db2Connection(Db2ConnectionTest.ConnectionString))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE(CLOB_TEXT) VALUES('blob is null')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE>("SELECT CLOB_TEXT, BLOB_DATA FROM DBG_TABLE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.CLOB_TEXT, Is.EqualTo("blob is null"));
					Assert.That(actual.DBCLOB_TEXT, Is.Null);
					Assert.That(actual.BLOB_DATA, Is.Null);
					Assert.That(actual.XML_BODY, Is.Null);
				}
			}
		}

		[Test]
		public void TestWriteAndReadXml()
		{
			using (var db = new Wintegra.Data.Db2Client.Db2Connection(Db2ConnectionTest.ConnectionString))
			{
				var xml = db.ToXmlDocument();

				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE(XML_BODY) VALUES(@XML_BODY)", new { XML_BODY = xml }, tn);
					var list = db.QueryObjects<DBG_TABLE>("SELECT XML_BODY FROM DBG_TABLE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.CLOB_TEXT, Is.Null);
					Assert.That(actual.DBCLOB_TEXT, Is.Null);
					Assert.That(actual.BLOB_DATA, Is.Null);
					Assert.That(actual.XML_BODY, Is.Not.Null);


					Assert.That(actual.XML_BODY.OuterXml
						.Substring("<?xml version=\"1.0\" encoding=\"UTF-16\"?>".Length),
						Is.EqualTo(xml.OuterXml
						.Substring("<?xml version=\"1.0\"?>".Length)));
				}
			}
		}

		[Test]
		public void TestReadNullXml()
		{
			using (var db = new Wintegra.Data.Db2Client.Db2Connection(Db2ConnectionTest.ConnectionString))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE(CLOB_TEXT) VALUES('xml is null')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE>("SELECT CLOB_TEXT, XML_BODY FROM DBG_TABLE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.CLOB_TEXT, Is.EqualTo("xml is null"));
					Assert.That(actual.DBCLOB_TEXT, Is.Null);
					Assert.That(actual.BLOB_DATA, Is.Null);
					Assert.That(actual.XML_BODY, Is.Null);
				}
			}
		}
	}
}