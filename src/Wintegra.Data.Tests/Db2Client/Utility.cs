using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Dapper;

namespace Wintegra.Data.Tests.Db2Client
{
	internal static class Utility
	{
		private static long m_ticks = 0;
		private static long Ticks { get { return ++m_ticks; } }

		internal const int FieldCharacterSize = 254;
		internal const int FieldGraphicSize = 127;
		
		public static char RandomAsciiChar()
		{
			const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
			var random = new Random((int)DateTime.UtcNow.Ticks);
			return chars[random.Next(chars.Length)];
		}

		public static string RandomAsciiString(int length)
		{
			const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
			var random = new Random((int)(DateTime.UtcNow.Ticks + Ticks));
			var sb = new StringBuilder(length);
			for (int i = 0; i < length; i++)
			{
				sb.Append(chars[random.Next(chars.Length)]);
			}
			return sb.ToString();
		}
		
		public static string RandomString(int length)
		{
			const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ";
			var random = new Random((int)(DateTime.UtcNow.Ticks + Ticks));
			var sb = new StringBuilder(length);
			for (int i = 0; i < length; i++)
			{
				sb.Append(chars[random.Next(chars.Length)]);
			}
			return sb.ToString();
		}

		public static XmlDocument ToXmlDocument<T>(this T input)
		{
			var serializer = new XmlSerializer(typeof(T), "http://schemas.wintegra.ru");

			XmlDocument document = null;

			using (var ms = new MemoryStream())
			{
				serializer.Serialize(ms, input);

				ms.Position = 0;

				var settings = new XmlReaderSettings();
				settings.IgnoreWhitespace = true;

				using (var reader = XmlReader.Create(ms, settings))
				{
					document = new XmlDocument();
					document.Load(reader);
				}
			}

			return document;
		}

		public static byte[] ToArrayBytes(this XmlDocument document)
		{
			using (var ms = new MemoryStream())
			{
				document.Save(ms);
				return ms.ToArray();
			}
		}

		public static IList<T> QueryObjects<T>(this IDbConnection conn, string query)
		{
			return (IList<T>)conn.Query<T>(query, buffered: true);
		}

		public static IList<T> QueryObjects<T>(this IDbConnection conn, string query, object paramObject)
		{
			return (IList<T>)conn.Query<T>(sql: query, param: paramObject, buffered: true);
		}
		
		public static IList<T> QueryObjects<T>(this IDbConnection conn, string query, object paramObject, IDbTransaction tn)
		{
			return (IList<T>)conn.Query<T>(sql: query, param: paramObject, buffered: true, transaction: tn);
		}

		public static IList<T> QueryObjects<T>(this IDbConnection conn, string query, T templateObject)
		{
			return conn.QueryObjects<T>(query);
		}
		public static IList<T> QueryObjects<T>(this IDbConnection conn, string query, object paramObject, T templateObject)
		{
			return conn.QueryObjects<T>(query, paramObject);
		}
		public static IList<T> QueryObjects<T>(this IDbConnection conn, string query, object paramObject, T templateObject, IDbTransaction tn)
		{
			return conn.QueryObjects<T>(query, paramObject, tn);
		}
	}

	[Serializable]
	public class XmlObjectData
	{
		public XmlObjectData() { }
		public string Field { get; set; }
	}

	[Serializable]
	public class SysUserData
	{
		public SysUserData() { }
		public string Login { get; set; }
		public string FirstName { get; set; }
		public string LastName { get; set; }
		public string MiddleName { get; set; }
		public int Division { get; set; }

		public string FIELD { get; set; }
	}

	class DBG_TABLE<T>
	{
		public T FIELD { get; set; }
		public char EMPTY { get; set; }
	}

	public class SqlQueryObject
	{
		public string DapperQuery { get; set; }
		public string Query { get; set; }

		public SqlQueryObject(string dapper, string sql)
		{
			this.DapperQuery = dapper;
			this.Query = sql;
		}
	}
	
	public class SqlCommandTextObject
	{
		public string CommandText { get; set; }
		public string Query { get; set; }

		public SqlCommandTextObject(string commandText, string query)
		{
			this.CommandText = commandText;
			this.Query = query;
		}
	}
}