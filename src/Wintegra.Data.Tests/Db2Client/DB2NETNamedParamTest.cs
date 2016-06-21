using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Xml;
using Dapper;
using NUnit.Framework;
using Wintegra.Data.Tests.Entities;

namespace Wintegra.Data.Tests.Db2Client
{
	[TestFixture]
	public class DB2NETNamedParamTest
	{
		[SetUp]
		public void SetUp()
		{
			Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
			Thread.CurrentThread.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");
		}

		[Test]
		public void EnableDB2NETNamedParam(
			[Values("...;DB2NETNamedParam=true;...",
				"...;DB2NETNamedParam=yes;...",
				"...;DB2NETNamedParam=1;...")] string connectionString)
		{
			var builder = new Wintegra.Data.jdbc.Db2ConnectionStringBuilder(connectionString);
			Assert.That(builder.NamedParameters, Is.True);
		}

		[Test]
		public void DisableDB2NETNamedParam(
			[Values("...;DB2NETNamedParam=false;...",
				"...;DB2NETNamedParam=no;...",
				"...;DB2NETNamedParam=0;...")] string connectionString)
		{
			var builder = new Wintegra.Data.jdbc.Db2ConnectionStringBuilder(connectionString);
			Assert.That(builder.NamedParameters, Is.False);
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
					db.Execute("INSERT INTO DBG_TABLE_CHARACTER(FIELD) VALUES(?)", new { character }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_CHARACTER(EMPTY) VALUES(?)", new { ch }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_VARCHAR(FIELD) VALUES(?)", new { varchar }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_VARCHAR(EMPTY) VALUES(?)", new { ch }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_CLOB(FIELD) VALUES(?)", new { clob }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_CLOB(EMPTY) VALUES(?)", new { ch }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_GRAPHIC(FIELD) VALUES(?)", new { graphic }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_GRAPHIC(EMPTY) VALUES(?)", new { ch }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_VARGRAPHIC(FIELD) VALUES(?)", new { vargraphic }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_VARGRAPHIC(EMPTY) VALUES(?)", new { ch }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_DBCLOB(FIELD) VALUES(?)", new { dbclob }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_DBCLOB(EMPTY) VALUES(?)", new { ch }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_BLOB(FIELD) VALUES(?)", new { blob }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_BLOB(EMPTY) VALUES(?)", new { ch }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_SMALLINT(FIELD) VALUES(?)", new { value }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_SMALLINT(EMPTY) VALUES(?)", new { ch }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_XML(FIELD) VALUES(?)", new { xml }, tn);
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
					db.Execute("INSERT INTO DBG_TABLE_XML(EMPTY) VALUES(?)", new { ch }, tn);
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

		#region Many params

		[Test]
		[Pairwise]
		public void TestAddAndReadFile(
			[Values("jdbc")] string type,
			[Values("AACH2", "BGD2M")] string id,
			[Values("2016.04.06 12:18:11", "2016.02.17 17:26:24")] string incomeString,
			[Values("AABH7", "BCAG3")] string packId,
			[Values(1,2,3,5,7,11,13,17)] int no,
			[Values("НИКОЛАЙ", "Светлана", "АННА", "Вячеслав")] string fname,
			[Values("Груздева", "ЗАЦЕПИН", "ИВАНОВА", "Давыдов")] string lname,
			[Values(null, "НИКИТИЧНА", "григорьевич", "Андреевна", "АЛЕКСАНДРОВИЧ")] string mname,
			[Values("30.09.1933", "08.09.1927", "15.10.1948")] string bdate,
			[Values("00000000000", "00000000101")] string snils,
			[Values("г.Москва ул.Осенний б-рд.XXX.корп.YY.кв.ZZZ", "матвеевская,XXX-YY")] string address,
			[Values(0, 2013, 2015)] long closeDate,
			[Values(1, 2, 3, 5)] long categoryCode)
		{
			var income = DateTime.Parse(incomeString);
			
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					{
						const string sql = @"INSERT INTO FILE_ENTRY(ID, INCOME, PACK_ID, NO, FNAME, LNAME, MNAME, BDATE, SNILS, ADDRESS, CLOSE_DATE, CATEGORY_ID) 
							VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
						
						var param = new
						{
							ID = id,
							INCOME = income,
							PACK_ID = packId,
							NO = no,
							FNAME = fname,
							LNAME = lname,
							MNAME = mname,
							BDATE = bdate,
							SNILS = snils,
							ADDRESS = address,
							CLOSE_DATE = closeDate,
							CATEGORY_ID = categoryCode,
						};
						db.Execute(sql, param, tn);
					}
					{
						const string sql = @"SELECT 
								ID, NO, PACK_ID, INCOME, FNAME, LNAME, MNAME, BDATE, ADDRESS, 
								CLOSE_DATE as CloseDate, 
								CATEGORY_ID as CategoryCode, 
								SNILS 
							FROM FILE_ENTRY";
						var param = new
						{
							ID = id,
						};

						var list = db.QueryObjects<FILE_ENTRY>(sql, param, tn);
						Assert.That(list, Is.Not.Null);

						var actual = list[0];
						Assert.That(actual, Is.Not.Null);
						Assert.That(actual.Income.ToLocalTime(), Is.EqualTo(income));
						Assert.That(actual.PACK_ID, Is.EqualTo(packId));
//						Assert.That(actual.No, Is.EqualTo(no));
//						Assert.That(actual.FName, Is.EqualTo(fname));
//						Assert.That(actual.LName, Is.EqualTo(lname));
//						Assert.That(actual.MName, Is.EqualTo(mname));
//						Assert.That(actual.BDate, Is.EqualTo(bdate));
//						Assert.That(actual.SNILS, Is.EqualTo(snils));
//						Assert.That(actual.Address, Is.EqualTo(address));
//						Assert.That(actual.CloseDate, Is.EqualTo(closeDate));
//						Assert.That(actual.CategoryCode, Is.EqualTo(categoryCode));
					}
					
				}
			}
		}

		#endregion
	}
}