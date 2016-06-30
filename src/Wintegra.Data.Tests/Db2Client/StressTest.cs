using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using NUnit.Framework;

namespace Wintegra.Data.Tests.Db2Client
{
	public class StressTest
	{
		[SetUp]
		public void SetUp()
		{
			Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
			Thread.CurrentThread.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");
		}

		[Test]
		[Ignore("Нагрузочный тест на DB2")]
		public void GetConectionCount(
			[Values("odbc", "jdbc")] string type,
			[Values(1,17,37,67)] int lenght)
		{
			const string sql = "select nvl(count(*),0) from FILE_ENTRY";

			var list = new List<IDbConnection>();
			long total = 0;

			var sw = new Stopwatch();
			var begin = DateTime.Now;
			sw.Start();
			try
			{
				do
				{
					var conn = Db2Driver.GetDbConnection(type);
					conn.Open();
					list.Add(conn);
					foreach (var cnn in list)
					{
						var count = cnn.Query<long>(sql).Single();
						total += count;
					}
					if (list.Count % 10 == 0) Console.WriteLine("Pool Size = " + list.Count);

				} while (list.Count <= lenght);
			}
			catch (Exception ex)
			{
				throw;
			}
			finally
			{
				foreach (var cnn in list) cnn.Dispose();
			}
			sw.Stop();
			Console.WriteLine("Max Pool Size = " + list.Count);
			Console.WriteLine("{0}: elapsed={1}ms or ~{2}ms, round={3}", type, sw.ElapsedMilliseconds, (DateTime.Now - begin).TotalMilliseconds, lenght);
		}
	}
}