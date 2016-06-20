using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Xml;
using Dapper;
using NUnit.Framework;

namespace Wintegra.Data.Tests.Db2Client
{
	[TestFixture]
	public class DapperTest
	{
		[SetUp]
		public void SetUp()
		{
			Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
			Thread.CurrentThread.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");
		}

		#region String data types

		#region CHARACTER

		[Test]
		public void TestWriteAndReadCharacter(
			[Values("odbc", "jdbc")] string type,
			[Values(1, 4, 64, 254)] int length)
		{
			string character = Utility.RandomAsciiString(length);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_CHARACTER(FIELD) VALUES(:CHARACTER_STRING)", new { CHARACTER_STRING = character }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD FROM DBG_TABLE_CHARACTER", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD.Length, Is.EqualTo(Utility.FieldCharacterSize));
					Assert.That(actual.FIELD, Does.StartWith(character));
					Assert.That(actual.FIELD, Is.EqualTo(character + new String(' ', Utility.FieldCharacterSize - character.Length)));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestReadNullCharacter([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_CHARACTER(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_CHARACTER", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

		#region VARCHAR

		[Test]
		public void TestWriteAndReadVarchar(
			[Values("odbc", "jdbc")] string type,
			[Values(1, 4, 64, 254)] int length)
		{
			string varchar = Utility.RandomAsciiString(length);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_VARCHAR(FIELD) VALUES(:VARCHAR_STRING)", new { VARCHAR_STRING = varchar }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD FROM DBG_TABLE_VARCHAR", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.EqualTo(varchar));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestReadNullVarchar([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_VARCHAR(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_VARCHAR", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

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
					db.Execute("INSERT INTO DBG_TABLE_CLOB(FIELD) VALUES(:CLOB_TEXT)", new { CLOB_TEXT = clob }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD FROM DBG_TABLE_CLOB", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.EqualTo(clob));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
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
					db.Execute("INSERT INTO DBG_TABLE_CLOB(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_CLOB", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

		#region GRAPHIC

		[Test]
		public void TestWriteAndReadGraphic(
			[Values("odbc", "jdbc")] string type,
			[Values(1, 4, 64, 127)] int length)
		{
			string graphic = Utility.RandomAsciiString(length);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_GRAPHIC(FIELD) VALUES(:GRAPHIC_STRING)", new { GRAPHIC_STRING = graphic }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD FROM DBG_TABLE_GRAPHIC", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD.Length, Is.EqualTo(Utility.FieldGraphicSize));
					Assert.That(actual.FIELD, Does.StartWith(graphic));
					Assert.That(actual.FIELD, Is.EqualTo(graphic + new String(' ', Utility.FieldGraphicSize - graphic.Length)));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestReadNullGraphic([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_GRAPHIC(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_GRAPHIC", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

		#region VARGRAPHIC

		[Test]
		public void TestWriteAndReadVargraphic(
			[Values("odbc", "jdbc")] string type,
			[Values(1, 4, 64, 1024)] int length)
		{
			string vargraphic = Utility.RandomAsciiString(length);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_VARGRAPHIC(FIELD) VALUES(:VARGRAPHIC_STRING)", new { VARGRAPHIC_STRING = vargraphic }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD FROM DBG_TABLE_VARGRAPHIC", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.EqualTo(vargraphic));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestReadNullVargraphic([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_VARGRAPHIC(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_VARGRAPHIC", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

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
					db.Execute("INSERT INTO DBG_TABLE_DBCLOB(FIELD) VALUES(:DBCLOB_TEXT)", new { DBCLOB_TEXT = dbclob }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD FROM DBG_TABLE_DBCLOB", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.EqualTo(dbclob));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
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
					db.Execute("INSERT INTO DBG_TABLE_DBCLOB(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_DBCLOB", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
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
					db.Execute("INSERT INTO DBG_TABLE_BLOB(FIELD) VALUES(:BLOB_DATA)", new { BLOB_DATA = blob }, tn);
					var list = db.QueryObjects<DBG_TABLE<byte[]>>("SELECT FIELD FROM DBG_TABLE_BLOB", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.EqualTo(blob));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
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
					db.Execute("INSERT INTO DBG_TABLE_BLOB(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<byte[]>>("SELECT FIELD, EMPTY FROM DBG_TABLE_BLOB", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}


		#endregion

		#endregion

		#region Numeric data types

		#region SMALLINT

		[Test]
		public void TestWriteAndReadSmallint(
			[Values("odbc", "jdbc")] string type,
			[Values(short.MinValue, -1, 0, 1, short.MaxValue)] short value)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_SMALLINT(FIELD) VALUES(:INT16)", new { INT16 = value }, tn);
					var list = db.QueryObjects<DBG_TABLE<short?>>("SELECT FIELD FROM DBG_TABLE_SMALLINT", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.EqualTo(value));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestReadNullSmallint([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_SMALLINT(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<short?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_SMALLINT", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

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

		private static readonly List<SqlQueryObject> DapperSet = new List<SqlQueryObject>()
		{
			new SqlQueryObject("INSERT INTO DBG_TABLE(XML_BODY) VALUES(:XML_BODY)","INSERT INTO DBG_TABLE(XML_BODY) VALUES(:XML_BODY)") ,
			new SqlQueryObject("INSERT INTO DBG_TABLE(XML_BODY) VALUES((:XML_BODY1,:XML_BODY2))","INSERT INTO DBG_TABLE(XML_BODY) VALUES(:XML_BODY)"),
			new SqlQueryObject("INSERT INTO DBG_TABLE(XML_BODY) VALUES((:XML_BODY11,:XML_BODY12),(:XML_BODY21,:XML_BODY22))","INSERT INTO DBG_TABLE(XML_BODY) VALUES(:XML_BODY1,:XML_BODY2)"),
		};

		[Test, TestCaseSource("DapperSet")]
		public void TestXmlExp(SqlQueryObject d)
		{
			string sql = Wintegra.Data.jdbc.Db2Command.XmlExp.Replace(d.DapperQuery, m => ":" + m.Groups["n"].Value);
			Assert.That(sql, Is.EqualTo(d.Query));
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
					db.Execute("INSERT INTO DBG_TABLE_XML(FIELD) VALUES(:XML_BODY)", new { XML_BODY = xml }, tn);
					var list = db.QueryObjects<DBG_TABLE<XmlDocument>>("SELECT FIELD FROM DBG_TABLE_XML", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));


					Assert.That(actual.FIELD.OuterXml
						.Substring("<?xml version=\"1.0\" encoding=\"UTF-16\"?>".Length),
						Is.EqualTo(xml.OuterXml
						.Substring("<?xml version=\"1.0\"?>".Length)));
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
					db.Execute("INSERT INTO DBG_TABLE_XML(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<XmlDocument>>("SELECT FIELD, EMPTY FROM DBG_TABLE_XML", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion
	}
}