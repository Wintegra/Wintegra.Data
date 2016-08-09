using System.Text;

namespace Wintegra.Data.Db2Client
{
	internal class Db2CommandText
	{
		internal static string ToODBC(string value)
		{
			var param = new StringBuilder();

			var cmd = new StringBuilder(value);
			for (int i = 0; i < cmd.Length; i++)
			{
				var ch = cmd[i];
				switch (ch)
				{
					// skip from -- to end line
					case '-':
						if (i + 1 >= cmd.Length) break;
						if (cmd[i + 1] != '-') break;
						for (i += 2; i < cmd.Length; i++)
						{
							var p = cmd[i];
							if (p == '\n') break;
						}
						break;
					// skip '...'
					case '\'':
						for (i++; i < cmd.Length; i++)
						{
							var p = cmd[i];
							if (p == '\'') break;
						}
						break;
					// skip "..."
					case '\"':
						for (i++; i < cmd.Length; i++)
						{
							var p = cmd[i];
							if (p == '\"') break;
						}
						break;
					// find param: escape to ?
					case ':':
					case '@':
						param.Length = 0;
						param.Capacity = 0;

						param.Append(cmd[i]);
						for (int j = i + 1; j < cmd.Length; j++)
						{
							var p = cmd[j];
							if (!char.IsLetterOrDigit(p) && p != '_') break;
							if (p == ',' || p == ')') break;
							param.Append(p);
						}
						cmd.Replace(param.ToString(), "?", i, param.Length); // @param => ?
						break;
				}
			}

			return cmd.ToString();
		}
	}
}