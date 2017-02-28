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
			Integer transactionId, Transaction transaction, Integer componentId, TransactionComponent nextTransactionComponent
	)
	{
		TreeMap<Integer, TransactionComponent> transactionComponentsNext = new TreeMap<Integer, TransactionComponent>(
				transaction.transactionComponents
		);
		transactionComponentsNext.put(
				componentId,
				nextTransactionComponent
		);
		this.transactionChange = new TransactionChange(
				transactionId,
				transaction,
				new Transaction(
						transaction.transactionPhase,
						transactionComponentsNext,
						transaction.function
				)
		);
	}

	public UnifiedAction(
			Integer transactionId,
			Transaction transaction, TransactionPhase transactionPhase,
			TransactionPhase transactionPhaseNext,
			ComponentPhase componentPhase, ComponentPhase componentPhaseNext
	)
	{
		assert transaction.transactionPhase == transactionPhase;
		TreeMap<Integer, TransactionComponent> transactionComponentsNext = new TreeMap<Integer, TransactionComponent>();
		for (SortedMap.Entry<Integer, TransactionComponent> transactionComponentEntry : transaction.transactionComponents.entrySet())
		{
			assert transactionComponentEntry.getValue().componentPhase == componentPhase;
			Integer key = transactionComponentEntry.getKey();
			TransactionComponent value = new TransactionComponent(transactionComponentEntry.getValue().versionToLock, componentPhaseNext);
			transactionComponentsNext.put(key, value);
		}
		this.transactionChange = new TransactionChange(
				transactionId,
				transaction,
				new Transaction(
						transactionPhaseNext,
						transactionComponentsNext,
						transaction.function
				)
		);
	}

	public ComponentChange componentChange;
	public TransactionChange transactionChange;
	public UnifiedDownloadRequest downloadRequest;
}
