package pl.rodia.jopama.data;

import java.util.SortedMap;
import java.util.TreeMap;

public class UnifiedAction
{

	public UnifiedAction(
			ComponentChange componentChange
	)
	{
		this.componentChange = componentChange;
		this.transactionChange = null;
		this.downloadRequest = null;
	}

	public UnifiedAction(
			TransactionChange transactionChange
	)
	{
		this.componentChange = null;
		this.transactionChange = transactionChange;
		this.downloadRequest = null;
	}

	public UnifiedAction(
			UnifiedDownloadRequest downloadRequest
	)
	{
		this.componentChange = null;
		this.transactionChange = null;
		this.downloadRequest = downloadRequest;
	}

	public UnifiedAction(
			Integer transactionId, ExtendedTransaction extendedTransaction, Integer componentId, TransactionComponent nextTransactionComponent
	)
	{
		TreeMap<Integer, TransactionComponent> transactionComponentsNext = new TreeMap<Integer, TransactionComponent>(
				extendedTransaction.transaction.transactionComponents
		);
		transactionComponentsNext.put(
				componentId,
				nextTransactionComponent
		);
		this.transactionChange = new TransactionChange(
				transactionId,
				extendedTransaction,
				new Transaction(
						extendedTransaction.transaction.transactionPhase,
						transactionComponentsNext,
						extendedTransaction.transaction.function
				)
		);
	}

	public UnifiedAction(
			Integer transactionId,
			ExtendedTransaction extendedTransaction, TransactionPhase transactionPhase,
			TransactionPhase transactionPhaseNext,
			ComponentPhase componentPhase, ComponentPhase componentPhaseNext
	)
	{
		assert extendedTransaction.transaction.transactionPhase == transactionPhase;
		TreeMap<Integer, TransactionComponent> transactionComponentsNext = new TreeMap<Integer, TransactionComponent>();
		for (SortedMap.Entry<Integer, TransactionComponent> transactionComponentEntry : extendedTransaction.transaction.transactionComponents.entrySet())
		{
			assert transactionComponentEntry.getValue().componentPhase == componentPhase;
			Integer key = transactionComponentEntry.getKey();
			TransactionComponent value = new TransactionComponent(transactionComponentEntry.getValue().versionToLock, componentPhaseNext);
			transactionComponentsNext.put(key, value);
		}
		this.transactionChange = new TransactionChange(
				transactionId,
				extendedTransaction,
				new Transaction(
						transactionPhaseNext,
						transactionComponentsNext,
						extendedTransaction.transaction.function
				)
		);
	}

	public ComponentChange componentChange;
	public TransactionChange transactionChange;
	public UnifiedDownloadRequest downloadRequest;
}
