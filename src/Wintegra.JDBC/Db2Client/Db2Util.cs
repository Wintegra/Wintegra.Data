using System;
using System.Globalization;
using BigDecimal = java.math.BigDecimal;
using Timestamp = java.sql.Timestamp;
using Time = java.sql.Time;
using Date = java.util.Date;

namespace Wintegra.JDBC.Db2Client
{
	internal static class Db2Util
	{
		// https://github.com/samskivert/ikvm-monotouch/blob/master/openjdk/sun/jdbc/odbc/JdbcOdbcUtils.java

		internal static StringComparer InvariantCaseIgnoringStringComparer { get { return StringComparer.InvariantCultureIgnoreCase; } }
		internal static CultureInfo en_US { get { return CultureInfo.GetCultureInfo("en-US"); } }

		internal static BigDecimal toBigDecimal(decimal value)
		{
			return new BigDecimal(value.ToString("G", en_US));
		}

		internal static decimal ToDecimal(BigDecimal value)
		{
			return decimal.Parse(value.toString(), NumberStyles.Float, en_US);
		}

		internal static Timestamp toTimestamp(DateTime value)
		{
			long javaMillis = getJavaMillis(value);
			int seconds = (int)(javaMillis / 1000);
			int nanos = (int)((javaMillis % 1000) * 1000000);
			return new Timestamp(70, 0, 1, 0, 0, seconds, nanos);
		}

		internal static long getJavaMillis(DateTime value)
		{
			long january_1st_1970 = 62135596800000L;
			return value.Ticks / 10000L - january_1st_1970;
		}

		internal static DateTime ToDateTime(Timestamp value)
		{
			long ticks = getNetTicks((Date)value);
			return new System.DateTime(ticks);
		}

		internal static DateTime ToDateTime(Date value)
		{
			long ticks = getNetTicks(value);
			return new System.DateTime(ticks);
		}

		internal static long getNetTicks(Date date)
		{
			// inverse from getJavaMillis
			long january_1st_1970 = 62135596800000L;
			return (date.getTime() + january_1st_1970) * 10000L;
		}

		internal static Time toTime(TimeSpan value)
		{
			return new Time(value.Hours, value.Minutes, value.Seconds);
		}

		internal static TimeSpan ToTimeSpan(Time value)
		{
			return new TimeSpan(value.getHours(), value.getMinutes(), value.getSeconds());
		}
	}
}