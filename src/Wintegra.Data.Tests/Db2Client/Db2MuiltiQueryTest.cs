using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using NUnit.Framework;
using Wintegra.Data.Tests.Entities;

namespace Wintegra.Data.Tests.Db2Client
{
	[TestFixture]
	public class Db2MuiltiQueryTest
	{
		[SetUp]
		public void SetUp()
		{
			Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
			Thread.CurrentThread.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");
		}

		[Test]
		public void TestSelectAndGetValue(
			[Values("odbc")] string type,
			[Values("67766215", "D519C704", "7F3D0A9B", "80971111")] string head_id,
			[Values("F04D1BA3", "5B2BF4F1", "044815E1", "DB8CE88E")] string line_id,
			[Values("simple", "any")] string note)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					InitEnviroment(db, tn, head_id, line_id, note);

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = 
@"SELECT
    h.ID,
    h.NOTE,
    l.ID,
    l.NOTE
FROM HEAD h
JOIN LINE l ON l.HEAD_ID=h.ID";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							Assert.That(reader.GetValue(0), Is.EqualTo(head_id));
							Assert.That(reader.GetValue(1), Is.EqualTo(note + ":head"));
							Assert.That(reader.GetValue(2), Is.EqualTo(line_id));
							Assert.That(reader.GetValue(3), Is.EqualTo(note + ":line"));
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

		[Test]
		public void TestSelectAndGetByName(
			[Values("odbc")] string type,
			[Values("67766215", "D519C704", "7F3D0A9B", "80971111")] string head_id,
			[Values("F04D1BA3", "5B2BF4F1", "044815E1", "DB8CE88E")] string line_id,
			[Values("simple", "any")] string note)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					InitEnviroment(db, tn, head_id, line_id, note);

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText =
@"SELECT
    h.ID,
    h.NOTE,
    l.ID,
    l.NOTE
FROM HEAD h
JOIN LINE l ON l.HEAD_ID=h.ID";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							Assert.That(reader["id"], Is.EqualTo(head_id));
							Assert.That(reader["note"], Is.EqualTo(note + ":head"));
							Assert.That(reader["id"], Is.EqualTo(line_id));
							Assert.That(reader["note"], Is.EqualTo(note + ":line"));
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

		[Test]
		public void TestSelectAndGetByAnyName(
			[Values("odbc")] string type,
			[Values("67766215", "D519C704", "7F3D0A9B", "80971111")] string head_id,
			[Values("F04D1BA3", "5B2BF4F1", "044815E1", "DB8CE88E")] string line_id,
			[Values("simple", "any")] string note)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					InitEnviroment(db, tn, head_id, line_id, note);

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText =
@"SELECT
    h.ID,
    h.NOTE,
    l.ID,
    l.NOTE
FROM HEAD h
JOIN LINE l ON l.HEAD_ID=h.ID";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							Assert.That(reader["id"], Is.EqualTo(head_id));
							Assert.That(reader["id"], Is.EqualTo(line_id));
							Assert.That(reader["note"], Is.EqualTo(note + ":line"));
							Assert.That(reader["id"], Is.EqualTo(head_id)); //???
							Assert.That(reader["id"], Is.EqualTo(line_id)); //???
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

		private static void InitEnviroment(IDbConnection db, IDbTransaction tn, string head_id, string line_id, string note)
		{
			#region Init environment

			using (var command = db.CreateCommand())
			{
				command.Transaction = tn;
				command.CommandText = "INSERT INTO HEAD(ID, NOTE) VALUES(:ID, :NOTE)";

				IDbDataParameter parameterObject = command.CreateParameter();
				parameterObject.ParameterName = "ID";
				parameterObject.Value = head_id;
				command.Parameters.Add(parameterObject);

				parameterObject = command.CreateParameter();
				parameterObject.ParameterName = "NOTE";
				parameterObject.Value = note + ":head";
				command.Parameters.Add(parameterObject);

				command.ExecuteNonQuery();
			}

			using (var command = db.CreateCommand())
			{
				command.Transaction = tn;
				command.CommandText = "INSERT INTO LINE(ID, HEAD_ID, NOTE) VALUES(:ID, :HEAD_ID, :NOTE)";

				IDbDataParameter parameterObject = command.CreateParameter();
				parameterObject.ParameterName = "ID";
				parameterObject.Value = line_id;
				command.Parameters.Add(parameterObject);

				parameterObject = command.CreateParameter();
				parameterObject.ParameterName = "HEAD_ID";
				parameterObject.Value = head_id;
				command.Parameters.Add(parameterObject);

				parameterObject = command.CreateParameter();
				parameterObject.ParameterName = "NOTE";
				parameterObject.Value = note + ":line";
				command.Parameters.Add(parameterObject);

				command.ExecuteNonQuery();
			}

			#endregion
		}

		[Test]
		public void TestQueryMultipleWithoutParameters(
			[Values("odbc")] string type,
			[Values("67766215", "D519C704", "7F3D0A9B", "80971111")] string packId)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO PACK_ENTRY(ID, CLOSE_DATE, CATEGORY_ID, GU_CODE, NO) VALUES(?, 2016, 1, 870000000, 1)", new { packId }, tn);

					#region multuSql

					const string multuSql =
@"SELECT 
	ID
	, SHORTNAME
	, NAME 
FROM CATEGORY_LIST;

SELECT 
    p.ID, 
    p.INCOME, 
    p.CLOSE_DATE as CloseDate, 
    p.CATEGORY_ID as CategoryCode, 
    p.GU_CODE as GuCode, 
    p.NO
FROM PACK_ENTRY p
FETCH FIRST 501 ROWS ONLY;";

					#endregion

					var list = new PACK_ENTRY_LIST(501);
					using (var result = db.QueryMultiple(multuSql, transaction: tn))
					{
						var catrgoryList = result.Read<CATEGORY_ENTRY>().GroupBy(l => l.ID).ToDictionary(g => g.Key, g => g.Single());
						foreach (var item in result.Read<PACK_ENTRY>())
						{
							item.CategoryName = catrgoryList[item.CategoryCode].ShortName;
							list.Add(item);
						}
					}

					foreach (PACK_ENTRY p in list)
					{
						Assert.That(p.ID, Is.EqualTo(packId));
						Assert.That(p.CloseDate, Is.EqualTo(2016));
						Assert.That(p.CategoryCode, Is.EqualTo(1));
						Assert.That(p.CategoryName, Is.EqualTo("По старости"));
						Assert.That(p.GuCode, Is.EqualTo(870000000));
						Assert.That(p.No, Is.EqualTo(1));
					}

				}
			}
		}

		[Test]
		public void TestQueryMultipleWithParameters(
			[Values("odbc")] string type,
			[Values("67766215", "D519C704", "7F3D0A9B", "80971111")] string packId,
			[Values(1, 2, 3, 4, 5)] int categoryCode)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					db.Execute("INSERT INTO PACK_ENTRY(ID, CLOSE_DATE, CATEGORY_ID, GU_CODE, NO) VALUES(?, 2016, ?, 870000000, 1)", new { packId, categoryCode }, tn);

					#region multuSql

					const string multuSql =
@"SELECT 
	ID
	, SHORTNAME
	, NAME 
FROM CATEGORY_LIST
WHERE ID=? AND 1=?;

SELECT 
    p.ID, 
    p.INCOME, 
    p.CLOSE_DATE as CloseDate, 
    p.CATEGORY_ID as CategoryCode, 
    p.GU_CODE as GuCode, 
    p.NO
FROM PACK_ENTRY p
WHERE p.CATEGORY_ID=?
FETCH FIRST 501 ROWS ONLY;";

					#endregion

					var list = new PACK_ENTRY_LIST(501);
					using (var result = db.QueryMultiple(multuSql, new { id = categoryCode, p = 1, categoryCode }, tn))
					{
						var catrgoryList = result.Read<CATEGORY_ENTRY>().GroupBy(l => l.ID).ToDictionary(g => g.Key, g => g.Single());
						foreach (var item in result.Read<PACK_ENTRY>())
						{
							item.CategoryName = catrgoryList[item.CategoryCode].ShortName;
							list.Add(item);
						}
					}

					foreach (PACK_ENTRY p in list)
					{
						Assert.That(p.ID, Is.EqualTo(packId));
						Assert.That(p.CloseDate, Is.EqualTo(2016));
						Assert.That(p.CategoryCode, Is.EqualTo(categoryCode));
						Assert.That(p.GuCode, Is.EqualTo(870000000));
						Assert.That(p.No, Is.EqualTo(1));
					}

				}
			}
		}
	}
}