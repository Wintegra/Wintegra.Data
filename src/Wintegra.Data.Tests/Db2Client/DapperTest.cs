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
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD.Length, Is.EqualTo(Utility.FieldCharacterSize));
					Assert.That(actual.FIELD, Does.StartWith(character));
					Assert.That(actual.FIELD, Is.EqualTo(character + new String(' ', Utility.FieldCharacterSize - character.Length)));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullCharacter([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_CHARACTER(FIELD, EMPTY) VALUES(:CHARACTER_STRING,:EMPTY)", new { CHARACTER_STRING = (string)null, EMPTY = ch}, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_CHARACTER", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
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
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(varchar));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullVarchar([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_VARCHAR(FIELD, EMPTY) VALUES(:VARCHAR_STRING,:EMPTY)", new { VARCHAR_STRING = (string)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_VARCHAR", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
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
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(clob));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullClob([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_CLOB(FIELD, EMPTY) VALUES(:CLOB_TEXT,:EMPTY)", new { CLOB_TEXT = (string)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_CLOB", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
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
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD.Length, Is.EqualTo(Utility.FieldGraphicSize));
					Assert.That(actual.FIELD, Does.StartWith(graphic));
					Assert.That(actual.FIELD, Is.EqualTo(graphic + new String(' ', Utility.FieldGraphicSize - graphic.Length)));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullGraphic([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_GRAPHIC(FIELD, EMPTY) VALUES(:GRAPHIC_STRING,:EMPTY)", new { GRAPHIC_STRING = (string)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_GRAPHIC", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
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
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(vargraphic));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullVargraphic([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_VARGRAPHIC(FIELD, EMPTY) VALUES(:VARGRAPHIC_STRING,:EMPTY)", new { VARGRAPHIC_STRING = (string)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_VARGRAPHIC", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
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
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(dbclob));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullDbclob([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DBCLOB(FIELD, EMPTY) VALUES(:DBCLOB_TEXT,:EMPTY)", new { DBCLOB_TEXT = (string)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_DBCLOB", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
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
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(blob));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullBlob([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_BLOB(FIELD, EMPTY) VALUES(:BLOB_DATA,:EMPTY)", new { BLOB_DATA = (byte[])null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<String>>("SELECT FIELD, EMPTY FROM DBG_TABLE_BLOB", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
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
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(value));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullSmallint([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_SMALLINT(FIELD, EMPTY) VALUES(:INT16,:EMPTY)", new { INT16 = (short?)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<short?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_SMALLINT", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
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

		#region INTEGER

		[Test]
		public void TestWriteAndReadInteger(
			[Values("odbc", "jdbc")] string type,
			[Values(int.MinValue, short.MinValue, -1, 0, 1, 7, short.MaxValue, int.MaxValue)] int value)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_INTEGER(FIELD) VALUES(:INT32)", new { INT32 = value }, tn);
					var list = db.QueryObjects<DBG_TABLE<int?>>("SELECT FIELD FROM DBG_TABLE_INTEGER", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(value));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullInteger([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_INTEGER(FIELD, EMPTY) VALUES(:INT32,:EMPTY)", new { INT32 = (int?)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<int?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_INTEGER", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		[Test]
		public void TestReadNullInteger([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_INTEGER(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<int?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_INTEGER", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

		#region BIGINT

		[Test]
		public void TestWriteAndReadBigint(
			[Values("odbc", "jdbc")] string type,
			[Values(long.MinValue, int.MinValue, short.MinValue, -1, 0, 1, 7, short.MaxValue, int.MaxValue, long.MaxValue)] long value)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_BIGINT(FIELD) VALUES(:INT64)", new { INT64 = value }, tn);
					var list = db.QueryObjects<DBG_TABLE<long?>>("SELECT FIELD FROM DBG_TABLE_BIGINT", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(value));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullBigint([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_BIGINT(FIELD, EMPTY) VALUES(:INT64,:EMPTY)", new { INT64 = (long?)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<long?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_BIGINT", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		[Test]
		public void TestReadNullBigint([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_BIGINT(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<long?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_BIGINT", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

		#region DECIMAL

		private static decimal GetDecimal(string valueString)
		{
			switch (valueString)
			{
				case "MinValue":
					return -792281625142643375935.4395034m;
				case "MinusOne":
					return -1m;
				case "Zero":
					return 0m;
				case "One":
					return 1m;
				case "MaxValue":
					return 792281625142643375935.4395034m;
				default:
					return decimal.Parse(valueString);
			}

		}

		[Test]
		public void TestWriteAndReadDecimal(
			[Values("odbc", "jdbc")] string type,
			[Values("MinValue", "MinusOne", "Zero", "One", "MaxValue")] string valueString)
		{
			decimal value = GetDecimal(valueString);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DECIMAL(FIELD) VALUES(:DECIMAL)", new { DECIMAL = value }, tn);
					var list = db.QueryObjects<DBG_TABLE<decimal?>>("SELECT FIELD FROM DBG_TABLE_DECIMAL", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					if ("odbc".Equals(type)) actual.FIELD /= 1e7m;
					Assert.That(actual.FIELD, Is.EqualTo(value));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullDecimal([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DECIMAL(FIELD, EMPTY) VALUES(:DECIMAL,:EMPTY)", new { DECIMAL = (decimal?)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<decimal?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_DECIMAL", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		[Test]
		public void TestReadNullDecimal([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DECIMAL(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<decimal?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_DECIMAL", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

		#region DECFLOAT

		// ODBC not supported DECFLOAT

		[Test]
		public void TestWriteAndReadDecfloat(
			[Values("jdbc")] string type,
			[Values("MinValue", "MinusOne", "Zero", "One", "MaxValue")] string valueString)
		{
			decimal value = GetDecimal(valueString);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DECFLOAT(FIELD) VALUES(:DECFLOAT)", new { DECFLOAT = value }, tn);
					var list = db.QueryObjects<DBG_TABLE<decimal?>>("SELECT FIELD FROM DBG_TABLE_DECFLOAT", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(value));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullDecfloat([Values("jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DECFLOAT(FIELD, EMPTY) VALUES(:DECFLOAT,:EMPTY)", new { DECFLOAT = (decimal?)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<decimal?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_DECFLOAT", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		[Test]
		public void TestReadNullDecfloat([Values("jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DECFLOAT(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<decimal?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_DECFLOAT", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

		#region REAL

		[Test]
		public void TestWriteAndReadReal(
			[Values("odbc", "jdbc")] string type,
			[Values(float.MinValue, -1.1f, -0.0f, 0.0f, 1.1f, float.MaxValue)] float value)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_REAL(FIELD) VALUES(:REAL)", new { REAL = value }, tn);
					var list = db.QueryObjects<DBG_TABLE<float?>>("SELECT FIELD FROM DBG_TABLE_REAL", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(value));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullReal([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_REAL(FIELD, EMPTY) VALUES(:REAL,:EMPTY)", new { REAL = (float?)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<float?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_REAL", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		[Test]
		public void TestReadNullReal([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_REAL(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<float?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_REAL", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

		#region DOUBLE

		[Test]
		public void TestWriteAndReadDouble(
			[Values("odbc", "jdbc")] string type,
			[Values(double.MinValue, -1.0, 0.0, 1.0, Double.MaxValue)] double value)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DOUBLE(FIELD) VALUES(:DOUBLE)", new { DOUBLE = value }, tn);
					var list = db.QueryObjects<DBG_TABLE<double?>>("SELECT FIELD FROM DBG_TABLE_DOUBLE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(value));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullDouble([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DOUBLE(FIELD, EMPTY) VALUES(:DOUBLE,:EMPTY)", new { DOUBLE = (double?)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<double?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_DOUBLE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		[Test]
		public void TestReadNullDouble([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DOUBLE(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<double?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_DOUBLE", new { }, tn);

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

		#region Date, time, and timestamp data types

		#region DATE

		[Test]
		public void TestWriteAndReadDate(
			[Values("odbc", "jdbc")] string type,
			[Values("2016-06-20")] string valueString)
		{
			var value = DateTime.Parse(valueString);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DATE(FIELD) VALUES(:DATE)", new { DATE = value }, tn);
					var list = db.QueryObjects<DBG_TABLE<DateTime?>>("SELECT FIELD FROM DBG_TABLE_DATE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					if ("jdbc".Equals(type)) actual.FIELD = actual.FIELD.Value.ToLocalTime();
					Assert.That(actual.FIELD, Is.EqualTo(value));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullDate([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DATE(FIELD, EMPTY) VALUES(:DATE,:EMPTY)", new { DATE = (DateTime?)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<DateTime?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_DATE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		[Test]
		public void TestReadNullDate([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_DATE(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<DateTime?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_DATE", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

		#region TIME

		[Test]
		public void TestWriteAndReadTime(
			[Values("odbc", "jdbc")] string type,
			[Values("16:49:05")] string valueString)
		{
			var value = TimeSpan.Parse(valueString);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_TIME(FIELD) VALUES(:TIME)", new { TIME = value }, tn);
					var list = db.QueryObjects<DBG_TABLE<TimeSpan?>>("SELECT FIELD FROM DBG_TABLE_TIME", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					Assert.That(actual.FIELD, Is.EqualTo(value));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullTime([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_TIME(FIELD, EMPTY) VALUES(:TIME,:EMPTY)", new { TIME = (TimeSpan?)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<TimeSpan?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_TIME", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		[Test]
		public void TestReadNullTime([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_TIME(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<TimeSpan?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_TIME", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		#endregion

		#region TIMESTAMP

		[Test]
		public void TestWriteAndReadTimestamp(
			[Values("odbc", "jdbc")] string type,
			[Values("2016-06-20 16:49:05.057")] string valueString)
		{
			var value = DateTime.Parse(valueString).ToUniversalTime();
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_TIMESTAMP(FIELD) VALUES(:TIMESTAMP)", new { TIMESTAMP = value }, tn);
					var list = db.QueryObjects<DBG_TABLE<DateTime?>>("SELECT FIELD FROM DBG_TABLE_TIMESTAMP", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual, Is.Not.Null);
					Assert.That(actual.FIELD, Is.Not.Null);
					if ("jdbc".Equals(type)) actual.FIELD = actual.FIELD.Value.ToLocalTime();
					Assert.That(actual.FIELD, Is.EqualTo(value));
					Assert.That(actual.EMPTY, Is.EqualTo((char)0));
				}
			}
		}

		[Test]
		public void TestWriteAndReadNullTimestamp([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_TIMESTAMP(FIELD, EMPTY) VALUES(:TIME,:EMPTY)", new { TIME = (DateTime?)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<DateTime?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_TIMESTAMP", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
				}
			}
		}

		[Test]
		public void TestReadNullTimestamp([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_TIMESTAMP(EMPTY) VALUES('" + ch + "')", new { }, tn);
					var list = db.QueryObjects<DBG_TABLE<TimeSpan?>>("SELECT FIELD, EMPTY FROM DBG_TABLE_TIMESTAMP", new { }, tn);

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
			string sql = Wintegra.JDBC.Db2Client.Db2Command.XmlExp.Replace(d.DapperQuery, m => ":" + m.Groups["n"].Value);
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
		[Ignore("System.NullReferenceException : Object reference not set to an instance of an object.")]
		public void TestWriteAndReadNullXml([Values("odbc", "jdbc")] string type)
		{
			char ch = Utility.RandomAsciiChar();

			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO DBG_TABLE_XML(FIELD, EMPTY) VALUES(:XML_BODY,:EMPTY)", new { XML_BODY = (XmlDocument)null, EMPTY = ch }, tn);
					var list = db.QueryObjects<DBG_TABLE<XmlDocument>>("SELECT FIELD, EMPTY FROM DBG_TABLE_XML", new { }, tn);

					Assert.That(list, Is.Not.Null);
					Assert.That(list.Count, Is.EqualTo(1));

					var actual = list[0];
					Assert.That(actual.FIELD, Is.Null);
					Assert.That(actual.EMPTY, Is.EqualTo(ch));
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