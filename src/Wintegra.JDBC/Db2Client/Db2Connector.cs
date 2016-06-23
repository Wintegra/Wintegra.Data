namespace Wintegra.JDBC.Db2Client
{
	enum TransactionStatus
	{
		Idle = 0,
		InTransactionBlock,
		InFailedTransactionBlock,
		Pending,
	}
}