package pl.rodia.jopama.core;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.Function;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.TransactionChange;
import pl.rodia.jopama.data.TransactionComponent;
import pl.rodia.jopama.data.TransactionPhase;
import pl.rodia.jopama.data.UnifiedAction;
import pl.rodia.jopama.data.UnifiedDownloadRequest;

public class TransactionAnalyzerImpl implements TransactionAnalyzer
{

	public TransactionAnalyzerImpl(
			LocalStorage proxyStorage
	)
	{
		super();
		this.proxyStorage = proxyStorage;
	}

	@Override
	public UnifiedAction getChange(
			ObjectId transactionId
	)
	{
		ExtendedTransaction extendedTransaction = this.proxyStorage.getTransaction(
				transactionId
		);
		if (
			extendedTransaction == null
		)
		{
			return new UnifiedAction(
					new UnifiedDownloadRequest(
							transactionId
					)
			);
		}
		for (SortedMap.Entry<ObjectId, TransactionComponent> transactionComponentEntry : extendedTransaction.transaction.transactionComponents.entrySet())
		{
			ExtendedComponent extendedComponent = this.proxyStorage.getComponent(
					transactionComponentEntry.getKey()
			);
			if (
				extendedComponent == null
			)
			{
				logger.trace(
						"TransactionAnalyzerImpl missing component downloading, transactionId: " + transactionId + " componentId: "
								+ transactionComponentEntry.getKey()
				);
				return new UnifiedAction(
						new UnifiedDownloadRequest(
								transactionId,
								transactionComponentEntry.getKey()
						)
				);
			}
		}
		ObjectId componentId;
		switch (extendedTransaction.transaction.transactionPhase)
		{
			case INITIAL:
				return new UnifiedAction(
						transactionId,
						extendedTransaction,
						TransactionPhase.INITIAL,
						TransactionPhase.LOCKING,
						ComponentPhase.INITIAL,
						ComponentPhase.NOT_LOCKED
				);
			case LOCKING:
				componentId = this.getFirstComponentInPhase(
						extendedTransaction.transaction,
						ComponentPhase.NOT_LOCKED
				);
				if (
					componentId != null
				)
				{
					return this.getComponentLockingAction(
							transactionId,
							extendedTransaction,
							componentId
					);
				}
				else
				{
					return new UnifiedAction(
							transactionId,
							extendedTransaction,
							TransactionPhase.LOCKING,
							TransactionPhase.UPDATING,
							ComponentPhase.LOCKED,
							ComponentPhase.NOT_UPDATED
					);
				}
			case UPDATING:
				componentId = this.getFirstComponentInPhase(
						extendedTransaction.transaction,
						ComponentPhase.NOT_UPDATED
				);
				if (
					componentId != null
				)
				{
					return this.getComponentUpdatingAction(
							transactionId,
							extendedTransaction,
							componentId
					);
				}
				else
				{
					return new UnifiedAction(
							transactionId,
							extendedTransaction,
							TransactionPhase.UPDATING,
							TransactionPhase.RELEASING,
							ComponentPhase.UPDATED,
							ComponentPhase.NOT_RELEASED
					);
				}
			case RELEASING:
				componentId = this.getFirstComponentInPhase(
						extendedTransaction.transaction,
						ComponentPhase.NOT_RELEASED
				);
				if (
					componentId != null
				)
				{
					return this.getComponentReleasingAction(
							transactionId,
							extendedTransaction,
							componentId
					);
				}
				else
				{
					return new UnifiedAction(
							transactionId,
							extendedTransaction,
							TransactionPhase.RELEASING,
							TransactionPhase.REMOVING,
							ComponentPhase.RELEASED,
							ComponentPhase.DONE
					);
				}
			case REMOVING:
				return new UnifiedAction(
						new TransactionChange(
								transactionId,
								extendedTransaction,
								null
						)
				);
			default:
				assert false;
				return null;
		}
	}

	private ObjectId getFirstComponentInPhase(
			Transaction transaction,
			ComponentPhase desiredPhase
	)
	{
		for (SortedMap.Entry<ObjectId, TransactionComponent> transactionComponentEntry : transaction.transactionComponents.entrySet())
		{
			if (
				transactionComponentEntry.getValue().componentPhase == desiredPhase
			)
			{
				return transactionComponentEntry.getKey();
			}
		}
		return null;
	}

	private UnifiedAction getComponentLockingAction(
			ObjectId transactionId,
			ExtendedTransaction extendedTransaction,
			ObjectId componentId
	)
	{
		assert extendedTransaction != null;
		assert componentId != null;
		TransactionComponent transactionComponent = extendedTransaction.transaction.transactionComponents.get(
				componentId
		);
		assert transactionComponent != null;
		ExtendedComponent extendedComponent = proxyStorage.getComponent(
				componentId
		);
		assert extendedComponent != null;
		assert transactionComponent.componentPhase == ComponentPhase.NOT_LOCKED;

		if (
			transactionComponent.versionToLock == null
					|| transactionComponent.versionToLock < extendedComponent.component.version
		)
		{
			return new UnifiedAction(
					transactionId,
					extendedTransaction,
					componentId,
					new TransactionComponent(
							extendedComponent.component.version,
							transactionComponent.componentPhase
					)
			);
		}
		else if (
			transactionComponent.versionToLock.equals(
					extendedComponent.component.version
			)
					&&
					extendedComponent.component.owner == null
		)
		{
			return new UnifiedAction(
					new ComponentChange(
							transactionId,
							componentId,
							extendedComponent,
							new Component(
									extendedComponent.component.version,
									transactionId,
									extendedComponent.component.value,
									extendedComponent.component.newValue
							)
					)
			);
		}
		else if (
			transactionId.equals(
					extendedComponent.component.owner
			)
					&& transactionComponent.versionToLock.equals(
							extendedComponent.component.version
					)
		)
		{
			return new UnifiedAction(
					transactionId,
					extendedTransaction,
					componentId,
					new TransactionComponent(
							transactionComponent.versionToLock,
							ComponentPhase.LOCKED
					)
			);
		}
		else
		{
			logger.trace(
					"TransactionAnalyzerImpl locking action impossible, downloading, transactionId: " + transactionId + " componentId: "
							+ componentId
			);
			return new UnifiedAction(
					new UnifiedDownloadRequest(
							transactionId,
							componentId
					)
			);
		}
	}

	private UnifiedAction getComponentUpdatingAction(
			ObjectId transactionId, ExtendedTransaction extendedTransaction, ObjectId componentId
	)
	{
		assert extendedTransaction != null;
		assert componentId != null;
		TransactionComponent transactionComponent = extendedTransaction.transaction.transactionComponents.get(
				componentId
		);
		assert transactionComponent != null;
		ExtendedComponent extendedComponent = proxyStorage.getComponent(
				componentId
		);
		assert extendedComponent != null;
		assert transactionComponent.componentPhase == ComponentPhase.NOT_UPDATED;
		if (
			!transactionId.equals(
					extendedComponent.component.owner
			)
		)
		{
			return new UnifiedAction(
					new UnifiedDownloadRequest(
							transactionId,
							componentId
					)
			);
		}
		else if (
			extendedComponent.component.newValue == null
		)
		{
			return new UnifiedAction(
					new ComponentChange(
							transactionId,
							componentId,
							extendedComponent,
							new Component(
									extendedComponent.component.version,
									extendedComponent.component.owner,
									extendedComponent.component.value,
									this.getNewValueForComponent(
											transactionId,
											extendedTransaction.transaction,
											componentId
									)
							)
					)
			);
		}
		else
		{
			logger.trace(
					"TransactionAnalyzerImpl updating action impossible, downloading, transactionId: " + transactionId + " componentId: "
							+ componentId
			);
			return new UnifiedAction(
					transactionId,
					extendedTransaction,
					componentId,
					new TransactionComponent(
							transactionComponent.versionToLock,
							ComponentPhase.UPDATED
					)
			);
		}
	}

	private Integer getNewValueForComponent(
			ObjectId transactionId, Transaction transaction, ObjectId componentId
	)
	{
		Function function = transaction.function;
		Map<ObjectId, Integer> functionArguments = new TreeMap<ObjectId, Integer>();
		for (SortedMap.Entry<ObjectId, TransactionComponent> transactionComponentEntry : transaction.transactionComponents.entrySet())
		{
			ComponentPhase componentPhase = transactionComponentEntry.getValue().componentPhase;
			assert componentPhase == ComponentPhase.NOT_UPDATED || componentPhase == ComponentPhase.UPDATED;
			ExtendedComponent extendedComponent = this.proxyStorage.getComponent(
					transactionComponentEntry.getKey()
			);
			assert extendedComponent != null;
			functionArguments.put(
					transactionComponentEntry.getKey(),
					extendedComponent.component.value
			);
		}
		Map<ObjectId, Integer> functionResult = function.execute(
				functionArguments
		);
		Integer result = functionResult.get(
				componentId
		);
		assert result != null;
		return result;
	}

	private UnifiedAction getComponentReleasingAction(
			ObjectId transactionId, ExtendedTransaction extendedTransaction, ObjectId componentId
	)
	{
		assert extendedTransaction != null;
		assert componentId != null;
		TransactionComponent transactionComponent = extendedTransaction.transaction.transactionComponents.get(
				componentId
		);
		assert transactionComponent != null;
		ExtendedComponent extendedComponent = proxyStorage.getComponent(
				componentId
		);
		assert extendedComponent != null;
		assert transactionComponent.componentPhase == ComponentPhase.NOT_RELEASED;
		if (
			transactionId.equals(
					extendedComponent.component.owner
			)
					&&
					extendedComponent.component.newValue != null
		)
		{
			assert extendedComponent.component.version.equals(
					transactionComponent.versionToLock
			);
			return new UnifiedAction(
					new ComponentChange(
							transactionId,
							componentId,
							extendedComponent,
							new Component(
									extendedComponent.component.version + 1,
									null,
									extendedComponent.component.newValue,
									null
							)
					)
			);
		}
		else if (
			extendedComponent.component.version > transactionComponent.versionToLock
		)
		{
			return new UnifiedAction(
					transactionId,
					extendedTransaction,
					componentId,
					new TransactionComponent(
							transactionComponent.versionToLock,
							ComponentPhase.RELEASED
					)
			);
		}
		else
		{
			logger.trace(
					"TransactionAnalyzerImpl releasing action impossible, downloading, transactionId: " + transactionId + " componentId: "
							+ componentId
			);
			return new UnifiedAction(
					new UnifiedDownloadRequest(
							transactionId,
							componentId
					)
			);
		}
	}

	LocalStorage proxyStorage;
	static final Logger logger = LogManager.getLogger();

}
