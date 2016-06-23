using NUnit.Framework;
using BigDecimal = java.math.BigDecimal;

namespace Wintegra.Data.Tests.Common
{
	[TestFixture]
	public class BigDecimalTest
	{
		[Test]
		public void FloatToBigDecimal(
			#region Values
			[Values(
				-101010101010101.2f, 
				-10101010101010.2f, 
				-1010101010101.2f, 
				-101010101010.2f, 
				-10101010101.2f, 
				-1010101010.2f, 
				-101010101.2f, 
				-10101010.2f, 
				-1010101.2f, 
				-101010.2f, 
				-10101.2f, 
				-1010.2f, 
				-101.2f,
				-10.2f, 
				-1.2f,
				-0.2f,
				-0.0f,
				0.0f,
				0.2f,
				1.2f, 
				10.2f, 
				101.2f, 
				1010.2f, 
				10101.2f,
				101010.2f, 
				1010101.2f,
				10101010.2f, 
				101010101.2f, 
				1010101010.2f, 
				10101010101.2f, 
				101010101010.2f, 
				1010101010101.2f, 
				10101010101010.2f)] 
			#endregion 
			float obj)
		{
			decimal value = global::System.Convert.ToDecimal(obj);
			var bd = Wintegra.JDBC.Db2Client.Db2Util.toBigDecimal(value);
			var dec = Wintegra.JDBC.Db2Client.Db2Util.ToDecimal(bd);
			Assert.That(dec, Is.EqualTo(value));
		}

		[Test]
		public void Decimal_MinValue()
		{
			decimal value = decimal.MinValue;
			var bd = Wintegra.JDBC.Db2Client.Db2Util.toBigDecimal(value);
			var dec = Wintegra.JDBC.Db2Client.Db2Util.ToDecimal(bd);
			Assert.That(dec, Is.EqualTo(value));
		}

		[Test]
		public void Decimal_MaxValue()
		{
			decimal value = decimal.MaxValue;
			var bd = Wintegra.JDBC.Db2Client.Db2Util.toBigDecimal(value);
			var dec = Wintegra.JDBC.Db2Client.Db2Util.ToDecimal(bd);
			Assert.That(dec, Is.EqualTo(value));
		}

		[Test]
		public void ToDecimalConvert(
			[Values("0E-7")] string value)
		{
			var bd = new BigDecimal(value);
			var dec = Wintegra.JDBC.Db2Client.Db2Util.ToDecimal(bd);
		}
	}
}