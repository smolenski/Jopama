package pl.rodia.jopama.core;

public abstract class TransactionProcessor
{
	
	public abstract void addTransaction(
			Integer transactionId
	);

	public abstract void removeTransaction(
			Integer transactionId
	);

	public abstract Integer getNumTransactions();
	
}
