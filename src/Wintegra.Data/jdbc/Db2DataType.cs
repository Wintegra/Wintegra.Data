namespace Wintegra.Data.jdbc
{
	public enum Db2DataType
	{
		Unknown = 0,

		Binary = 1,

		Single = 15,
		Decimal = 7,
		Double = 8,

		Int16 = 10,
		Int32 = 11,
		Int64 = 12,

		Date = 5,
		DateTime = 6,
		Time = 17,

		String = 16,
	}
}