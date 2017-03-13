package pl.rodia.jopama.core;

import pl.rodia.mpf.Task;

public abstract class TransactionProcessor
{
	
	public abstract void addTransaction(
			Integer transactionId,
			Task transactionDone
	);

}
