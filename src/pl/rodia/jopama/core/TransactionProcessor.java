package pl.rodia.jopama.core;

import pl.rodia.jopama.data.ObjectId;
import pl.rodia.mpf.Task;

public abstract class TransactionProcessor
{
	
	public abstract void addTransaction(
			ObjectId transactionId,
			Task transactionDone
	);

}
