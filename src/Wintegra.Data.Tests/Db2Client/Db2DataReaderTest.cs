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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_CHARACTER(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = character;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_CHARACTER";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (String)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual.Length, Is.EqualTo(Utility.FieldCharacterSize));
							Assert.That(actual, Does.StartWith(character));
							Assert.That(actual, Is.EqualTo(character + new String(' ', Utility.FieldCharacterSize - character.Length)));
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_CHARACTER(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_CHARACTER";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_VARCHAR(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = varchar;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_VARCHAR";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (String)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(varchar));
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_VARCHAR(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_VARCHAR";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_GRAPHIC(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = graphic;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_GRAPHIC";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (String)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual.Length, Is.EqualTo(Utility.FieldGraphicSize));
							Assert.That(actual, Does.StartWith(graphic));
							Assert.That(actual, Is.EqualTo(graphic + new String(' ', Utility.FieldGraphicSize - graphic.Length)));
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_GRAPHIC(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_GRAPHIC";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_VARGRAPHIC(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = vargraphic;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_VARGRAPHIC";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (String)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(vargraphic));
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_VARGRAPHIC(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_VARGRAPHIC";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_SMALLINT(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_SMALLINT";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (short)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
				}
			}
		}

		[Test]
		public void TestWriteAndReadSmallintAsInt16(
			[Values("odbc", "jdbc")] string type,
			[Values(short.MinValue, -1, 0, 1, short.MaxValue)] short value)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_SMALLINT(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_SMALLINT";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = reader.GetInt16(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_SMALLINT(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_SMALLINT";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_INTEGER(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_INTEGER";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (int)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
				}
			}
		}

		[Test]
		public void TestWriteAndReadIntegerAsInt32(
			[Values("odbc", "jdbc")] string type,
			[Values(int.MinValue, short.MinValue, -1, 0, 1, 7, short.MaxValue, int.MaxValue)] int value)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_INTEGER(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_INTEGER";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = reader.GetInt32(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_INTEGER(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_INTEGER";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_BIGINT(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_BIGINT";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (long)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
				}
			}
		}

		[Test]
		public void TestWriteAndReadBigintAsInt64(
			[Values("odbc", "jdbc")] string type,
			[Values(int.MinValue, short.MinValue, -1, 0, 1, 7, short.MaxValue, int.MaxValue)] int value)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_BIGINT(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_BIGINT";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = reader.GetInt64(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_BIGINT(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_BIGINT";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DECIMAL(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_DECIMAL";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (decimal)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							if ("odbc".Equals(type)) actual /= 1e7m;
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
				}
			}
		}

		[Test]
		public void TestWriteAndReadDecimalAsDecimal(
			[Values("odbc", "jdbc")] string type,
			[Values("MinValue", "MinusOne", "Zero", "One", "MaxValue")] string valueString)
		{
			decimal value = GetDecimal(valueString);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DECIMAL(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_DECIMAL";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = reader.GetDecimal(0);
							Assert.That(actual, Is.Not.Null);
							if ("odbc".Equals(type)) actual /= 1e7m;
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DECIMAL(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_DECIMAL";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DECFLOAT(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_DECFLOAT";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (decimal)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
				}
			}
		}

		[Test]
		public void TestWriteAndReadDecfloatAsDecimal(
			[Values("jdbc")] string type,
			[Values("MinValue", "MinusOne", "Zero", "One", "MaxValue")] string valueString)
		{
			decimal value = GetDecimal(valueString);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DECFLOAT(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_DECFLOAT";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = reader.GetDecimal(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DECFLOAT(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_DECFLOAT";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_REAL(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_REAL";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (float)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value).Within(0.0000001));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
				}
			}
		}

		[Test]
		public void TestWriteAndReadRealAsFloat(
			[Values("odbc", "jdbc")] string type,
			[Values(float.MinValue, -1.1f, -0.0f, 0.0f, 1.1f, float.MaxValue)] float value)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_REAL(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_REAL";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = reader.GetFloat(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
				}
			}
		}

		[Test]
		public void TestReadNullReal([Values("odbc","jdbc")] string type)
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
						command.CommandText = "INSERT INTO DBG_TABLE_REAL(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_REAL";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DOUBLE(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_DOUBLE";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (double)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
				}
			}
		}

		[Test]
		public void TestWriteAndReadDoubleAsDouble(
			[Values("odbc", "jdbc")] string type,
			[Values(double.MinValue, -1.0, 0.0, 1.0, Double.MaxValue)] double value)
		{
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DOUBLE(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_DOUBLE";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = reader.GetDouble(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DOUBLE(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_DOUBLE";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DATE(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_DATE";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (DateTime)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							if ("jdbc".Equals(type)) actual = actual.ToLocalTime();
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
				}
			}
		}

		[Test]
		public void TestWriteAndReadDateAsDateTime(
			[Values("odbc", "jdbc")] string type,
			[Values("2016-06-20")] string valueString)
		{
			var value = DateTime.Parse(valueString);
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DATE(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_DATE";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = reader.GetDateTime(0);
							Assert.That(actual, Is.Not.Null);
							if ("jdbc".Equals(type)) actual = actual.ToLocalTime();
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_DATE(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_DATE";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_TIME(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_TIME";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (TimeSpan)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
				}
			}
		}

		[Test]
		public void TestWriteAndReadTimeAsTimespanp(
			[Values("jdbc")] string type,
			[Values("16:49:05")] string valueString)
		{
			var value = TimeSpan.Parse(valueString);
			using (var jdbc = Db2Driver.GetDbConnection(type))
			{
				var db = jdbc as Wintegra.JDBC.Db2Client.Db2Connection;
				Assert.That(db, Is.Not.Null);

				db.Open();
				using (var jtn = db.BeginTransaction())
				{
					var tn = jtn as Wintegra.JDBC.Db2Client.Db2Transaction;
					Assert.That(tn, Is.Not.Null);

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_TIME(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_TIME";

						using (var jreader = command.ExecuteReader())
						{
							var reader = jreader as Wintegra.JDBC.Db2Client.Db2DataReader;
							Assert.That(reader, Is.Not.Null);

							Assert.That(reader.Read(), Is.True);
							var actual = reader.GetTime(0);
							Assert.That(actual, Is.Not.Null);
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_TIME(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_TIME";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_TIMESTAMP(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_TIMESTAMP";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = (DateTime)reader.GetValue(0);
							Assert.That(actual, Is.Not.Null);
							if ("jdbc".Equals(type)) actual = actual.ToLocalTime();
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
				}
			}
		}

		[Test]
		public void TestWriteAndReadTimestampAsDateTime(
			[Values("odbc", "jdbc")] string type,
			[Values("2016-06-20 16:49:05.057")] string valueString)
		{
			var value = DateTime.Parse(valueString).ToUniversalTime();
			using (var db = Db2Driver.GetDbConnection(type))
			{
				db.Open();
				using (var tn = db.BeginTransaction())
				{
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_TIMESTAMP(FIELD) VALUES(:FIELD)";

						IDbDataParameter parameterObject = command.CreateParameter();
						parameterObject.ParameterName = "FIELD";
						parameterObject.Value = value;
						command.Parameters.Add(parameterObject);

						var rowCount = command.ExecuteNonQuery();
						Assert.That(rowCount, Is.EqualTo(1));
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT FIELD FROM DBG_TABLE_TIMESTAMP";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var actual = reader.GetDateTime(0);
							Assert.That(actual, Is.Not.Null);
							if ("jdbc".Equals(type)) actual = actual.ToLocalTime();
							Assert.That(actual, Is.EqualTo(value));
							Assert.That(reader.NextResult(), Is.False);
						}
					}
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
					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "INSERT INTO DBG_TABLE_TIMESTAMP(EMPTY) VALUES('" + ch + "')";

						command.ExecuteNonQuery();
					}

					using (var command = db.CreateCommand())
					{
						command.Transaction = tn;
						command.CommandText = "SELECT EMPTY, FIELD FROM DBG_TABLE_TIMESTAMP";

						using (var reader = command.ExecuteReader())
						{
							Assert.That(reader.Read(), Is.True);
							var msg = reader.GetChar(0);
							var isNull = reader.IsDBNull(1);
							Assert.That(msg, Is.EqualTo(ch));
							Assert.That(isNull, Is.True);
							Assert.That(reader.NextResult(), Is.False);
						}

					}
				}
			}
		}

		#endregion

		#endregion 

		#region XML data type

		[Test]
		public void TestWriteAndReadXml(
			[Values("jdbc")] string type,
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