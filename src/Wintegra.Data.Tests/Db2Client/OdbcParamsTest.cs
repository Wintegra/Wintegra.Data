using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using NUnit.Framework;

namespace Wintegra.Data.Tests.Db2Client
{
	[TestFixture]
	public class OdbcParamsTest
	{
		private static readonly Regex OdbcCommandTimeout = new Regex(@"OdbcCommandTimeout=(?<it>\d+)", RegexOptions.Compiled | RegexOptions.CultureInvariant);

		[SetUp]
		public void SetUp()
		{
			Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
			Thread.CurrentThread.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");
		}

		[Test]
		public void EnableCommandTimeout(
			[Values("...;OdbcCommandTimeout=0;...",
				"...;OdbcCommandTimeout=5;...",
				"...;OdbcCommandTimeout=93;...")] string connectionString)
		{
			var value = int.Parse(OdbcCommandTimeout.Match(connectionString).Groups["it"].Value);
			var builder = new Wintegra.Data.Db2Client.Db2ConnectionStringBuilder(connectionString);
			Assert.That(builder.CommandTimeout, Is.EqualTo(value));
		}

		[Test]
		public void DisableCommandTimeout(
			[Values("...;DB2NETNamedParam=1;...",
				"...;HostVarParameters=1;...")] string connectionString)
		{
			var builder = new Wintegra.Data.Db2Client.Db2ConnectionStringBuilder(connectionString);
			Assert.That(builder.CommandTimeout, Is.EqualTo(30));
		}
	}
}