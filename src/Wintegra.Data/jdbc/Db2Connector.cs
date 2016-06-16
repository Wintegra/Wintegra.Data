namespace Wintegra.Data.jdbc
{
	enum TransactionStatus
	{
		Idle = 0,
		InTransactionBlock,
		InFailedTransactionBlock,
		Pending,
	}
}