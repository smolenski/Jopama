package pl.rodia.jopama;

public abstract class TransactionProcessor
{

	public abstract void processTransaction(
			Integer transactionId
	);

	public abstract void removeTransaction(
			Integer transactionId
	);

}
