using System.Data;
using System.Globalization;
using System.Threading;
using NUnit.Framework;

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
			[Values("odbc", "jdbc")] string type,
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
			[Values("odbc", "jdbc")] string type,
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
			[Values("odbc", "jdbc")] string type,
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
	}
}