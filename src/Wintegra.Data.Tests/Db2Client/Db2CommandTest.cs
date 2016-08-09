using System.Collections.Generic;
using System.Data.Odbc;
using System.Globalization;
using System.Threading;
using NUnit.Framework;

namespace Wintegra.Data.Tests.Db2Client
{
	[TestFixture]
	public class Db2CommandTest
	{
		[SetUp]
		public void SetUp()
		{
			Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
			Thread.CurrentThread.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");
		}

		#region DapperSet
		private static readonly List<SqlCommandTextObject> DapperSet = new List<SqlCommandTextObject>()
		{
			new SqlCommandTextObject("INSERT INTO DBG_TABLE(FIELD) VALUES(:FIELD)","INSERT INTO DBG_TABLE(FIELD) VALUES(?)") ,
			new SqlCommandTextObject("INSERT INTO DBG_TABLE(FIELD) VALUES(@FIELD)","INSERT INTO DBG_TABLE(FIELD) VALUES(?)") ,
			new SqlCommandTextObject("INSERT INTO DBG_TABLE(FIELD1,FIELD2) VALUES(:FIELD1,:FIELD2)","INSERT INTO DBG_TABLE(FIELD1,FIELD2) VALUES(?,?)") ,
			new SqlCommandTextObject("INSERT INTO DBG_TABLE(FIELD1,FIELD2) VALUES(@FIELD1,@FIELD2)","INSERT INTO DBG_TABLE(FIELD1,FIELD2) VALUES(?,?)") ,
			new SqlCommandTextObject("-- :any","-- :any") ,
			new SqlCommandTextObject("--  :any","--  :any") ,
			new SqlCommandTextObject("--s  :any","--s  :any") ,
			new SqlCommandTextObject("-- some:any","-- some:any"),
			new SqlCommandTextObject("-- some:","-- some:"),
			new SqlCommandTextObject("':any'","':any'"),
			new SqlCommandTextObject("' :any'","' :any'"),
			new SqlCommandTextObject("'any :any'","'any :any'"),
			new SqlCommandTextObject("' :any '","' :any '"),
			new SqlCommandTextObject(@"""json:Array""",@"""json:Array"""),
			new SqlCommandTextObject(@""" json:Array""",@""" json:Array"""),
			new SqlCommandTextObject(@"""json :Array""",@"""json :Array"""),
			new SqlCommandTextObject(@"XMLTABLE(XMLNAMESPACES(DEFAULT 'http://schemas.wintegra.ru')",@"XMLTABLE(XMLNAMESPACES(DEFAULT 'http://schemas.wintegra.ru')"),
			new SqlCommandTextObject(
@"-- :any
select * from t where id=@id",
@"-- :any
select * from t where id=?") ,
			new SqlCommandTextObject(@"SELECT TO_CHAR(INCOME,':YYYY-MMDD HH24:MI:SS') FROM (VALUES (:TIMESTAMP,:A)) AS T(INCOME, TODO)",@"SELECT TO_CHAR(INCOME,':YYYY-MMDD HH24:MI:SS') FROM (VALUES (?,?)) AS T(INCOME, TODO)"),
			new SqlCommandTextObject(
@"SELECT TO_CHAR(INCOME,'YYYY-MM-DD :Y HH24:MI:SS'), @E, --t
':T' FROM (VALUES (:TIMESTAMP,:A)) AS T(INCOME, TODO)
-- :CX",
@"SELECT TO_CHAR(INCOME,'YYYY-MM-DD :Y HH24:MI:SS'), ?, --t
':T' FROM (VALUES (?,?)) AS T(INCOME, TODO)
-- :CX"),
			new SqlCommandTextObject(@"INSERT INTO DBG_TABLE_BLOB(FIELD) VALUES(:BLOB_DATA)",@"INSERT INTO DBG_TABLE_BLOB(FIELD) VALUES(?)"),
		};
		#endregion

		[Test, TestCaseSource("DapperSet")]
		public void TestCommandTextWithParam(SqlCommandTextObject d)
		{
			var cmd = new Wintegra.Data.Db2Client.Db2Command(new OdbcCommand());
			cmd.CommandText = d.CommandText;
			Assert.That(cmd.CommandText, Is.EqualTo(d.Query));
		}
	}
}