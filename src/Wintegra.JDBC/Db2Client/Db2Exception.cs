using System;
using System.Data.Common;
using SQLException = java.sql.SQLException;
using PrintStream = java.io.PrintStream;
using ByteArrayOutputStream = java.io.ByteArrayOutputStream;

namespace Wintegra.JDBC.Db2Client
{
	[Serializable]
	public sealed class Db2Exception : DbException
	{
		internal static string GetJavaStackTrace(SQLException se)
		{
			using (var stream = new ByteArrayOutputStream())
			using (var ps = new PrintStream(stream))
			{
				se.printStackTrace(ps);
				var buffer = stream.toByteArray();
				return System.Text.Encoding.UTF8.GetString(buffer, 0, buffer.Length);
			}
		}

		internal Db2Exception(SQLException se)
			: base(GetJavaStackTrace(se), se)
		{
		}
	}
}