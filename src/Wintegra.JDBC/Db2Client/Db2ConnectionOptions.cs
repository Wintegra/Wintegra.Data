using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Wintegra.JDBC.Db2Client
{
	internal class Db2ConnectionOptions
	{
		private static readonly Regex DB2NETNamedParam = new Regex(@"DB2NETNamedParam=(true|yes|1)", RegexOptions.Compiled | RegexOptions.CultureInvariant);


		private readonly string _connectionString;
		internal readonly Dictionary<string, object> KeyChain;

		public Db2ConnectionOptions(string connectionString)
		{
			_connectionString = connectionString;
			KeyChain = new Dictionary<string, object>();
			Init();
		}

		private void Init()
		{
			var match = DB2NETNamedParam.Match(_connectionString);
			KeyChain.Add("DB2NETNamedParam", (bool)(match.Success));
		}
	}
}