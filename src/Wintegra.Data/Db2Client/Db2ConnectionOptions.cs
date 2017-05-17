using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Wintegra.Data.Db2Client
{
	internal class Db2ConnectionOptions
	{
		private static readonly Regex OdbcCommandTimeout = new Regex(@"OdbcCommandTimeout=(?<it>\d+)", RegexOptions.Compiled | RegexOptions.CultureInvariant);


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
			var match = OdbcCommandTimeout.Match(_connectionString);
			if (match.Success)
			{
				KeyChain.Add("OdbcCommandTimeout", int.Parse(match.Groups["it"].Value));
			}
		}
	}
}