using System;
using System.Linq;
using NUnit.Framework;

namespace Wintegra.Data.Tests.Db2Client
{
	[TestFixture]
	public class SystemFuncTest
	{
		[Test]
		public void TO_CHAR([Values("odbc")] string type,
			[Values("2016-06-20 16:49:05.057", "2016-08-08 13:59:26.784")] string valueString)
		{
			var value = DateTime.Parse(valueString).ToUniversalTime();
			string toChar;
			using (var db = Db2Driver.GetDbConnection(type))
			{
				toChar = db.QueryObjects<string>("SELECT TO_CHAR(INCOME,'YYYY-MM-DD HH24:MI:SS') FROM (VALUES (:TIMESTAMP)) AS T(INCOME)", new { TIMESTAMP = value }).First();
			}
			Assert.That(toChar, Is.EqualTo(value.ToString("yyyy-MM-dd HH:mm:ss")));
		}
	}
}