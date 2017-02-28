package pl.rodia.jopama.integration1;

import pl.rodia.jopama.TransactionProcessor;

public class TransactionProcessorWrapper extends TransactionProcessor
{
	public TransactionProcessorWrapper()
	{
		this.targetTransactionProcessor = null;
	}
	
	public void setTargetTransactionProcessor(TransactionProcessor targetTransactionProcessor)
	{
		this.targetTransactionProcessor = targetTransactionProcessor;
	}

	@Override
	public void processTransaction(
			Integer transactionId
	)
	{
		assert this.targetTransactionProcessor != null;
		this.targetTransactionProcessor.processTransaction(transactionId);
	}

	@Override
	public void removeTransaction(
			Integer transactionId
	)
	{
		assert false;
	}
	
	TransactionProcessor targetTransactionProcessor;

}
