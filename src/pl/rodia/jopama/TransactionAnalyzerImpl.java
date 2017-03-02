package pl.rodia.jopama;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.Function;
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
			Integer transactionId
	)
	{
		Transaction transaction = this.proxyStorage.getTransaction(
				transactionId
		);
		if (
			transaction == null
		)
		{
			return new UnifiedAction(
					new UnifiedDownloadRequest(
							transactionId
					)
			);
		}
		for (SortedMap.Entry<Integer, TransactionComponent> transactionComponentEntry : transaction.transactionComponents.entrySet())
		{
			Component component = this.proxyStorage.getComponent(
					transactionComponentEntry.getKey()
			);
			if (
				component == null
			)
			{
				logger.trace("TransactionAnalyzerImpl missing component downloading, transactionId: " + transactionId + " componentId: " + transactionComponentEntry.getKey());
				return new UnifiedAction(
						new UnifiedDownloadRequest(
								transactionId,
								transactionComponentEntry.getKey()
						)
				);
			}
		}
		Integer componentId;
		switch (transaction.transactionPhase)
		{
			case INITIAL:
				return new UnifiedAction(
						transactionId,
						transaction,
						TransactionPhase.INITIAL,
						TransactionPhase.LOCKING,
						ComponentPhase.INITIAL,
						ComponentPhase.NOT_LOCKED
				);
			case LOCKING:
				componentId = this.getFirstComponentInPhase(
						transaction,
						ComponentPhase.NOT_LOCKED
				);
				if (
					componentId != null
				)
				{
					return this.getComponentLockingAction(
							transactionId,
							transaction,
							componentId
					);
				}
				else
				{
					return new UnifiedAction(
							transactionId,
							transaction,
							TransactionPhase.LOCKING,
							TransactionPhase.UPDATING,
							ComponentPhase.LOCKED,
							ComponentPhase.NOT_UPDATED
					);
				}
			case UPDATING:
				componentId = this.getFirstComponentInPhase(
						transaction,
						ComponentPhase.NOT_UPDATED
				);
				if (
					componentId != null
				)
				{
					return this.getComponentUpdatingAction(
							transactionId,
							transaction,
							componentId
					);
				}
				else
				{
					return new UnifiedAction(
							transactionId,
							transaction,
							TransactionPhase.UPDATING,
							TransactionPhase.RELEASING,
							ComponentPhase.UPDATED,
							ComponentPhase.NOT_RELEASED
					);
				}
			case RELEASING:
				componentId = this.getFirstComponentInPhase(
						transaction,
						ComponentPhase.NOT_RELEASED
				);
				if (
					componentId != null
				)
				{
					return this.getComponentReleasingAction(
							transactionId,
							transaction,
							componentId
					);
				}
				else
				{
					return new UnifiedAction(
							transactionId,
							transaction,
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
								transaction,
								null
						)
				);
			default:
				assert false;
				return null;
		}
	}

	private Integer getFirstComponentInPhase(
			Transaction transaction,
			ComponentPhase desiredPhase
	)
	{
		for (SortedMap.Entry<Integer, TransactionComponent> transactionComponentEntry : transaction.transactionComponents.entrySet())
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
			Integer transactionId,
			Transaction transaction,
			Integer componentId
	)
	{
		assert transaction != null;
		assert componentId != null;
		TransactionComponent transactionComponent = transaction.transactionComponents.get(
				componentId
		);
		assert transactionComponent != null;
		Component component = proxyStorage.getComponent(
				componentId
		);
		assert component != null;
		assert transactionComponent.componentPhase == ComponentPhase.NOT_LOCKED;

		if (
			transactionComponent.versionToLock == null
					|| transactionComponent.versionToLock < component.version
		)
		{
			return new UnifiedAction(
					transactionId,
					transaction,
					componentId,
					new TransactionComponent(
							component.version,
							transactionComponent.componentPhase
					)
			);
		}
		else if (
			transactionComponent.versionToLock.equals(component.version)
					&& component.owner == null
		)
		{
			return new UnifiedAction(
					new ComponentChange(
							transactionId,
							componentId,
							component,
							new Component(
									component.version,
									transactionId,
									component.value,
									component.newValue
							)
					)
			);
		}
		else if (
			component.owner.equals(transactionId)
					&& transactionComponent.versionToLock.equals(component.version)
		)
		{
			return new UnifiedAction(
					transactionId,
					transaction,
					componentId,
					new TransactionComponent(
							transactionComponent.versionToLock,
							ComponentPhase.LOCKED
					)
			);
		}
		else
		{
			logger.trace("TransactionAnalyzerImpl locking action impossible, downloading, transactionId: " + transactionId + " componentId: " + componentId);
			return new UnifiedAction(
					new UnifiedDownloadRequest(
							transactionId,
							componentId
					)
			);
		}
	}

	private UnifiedAction getComponentUpdatingAction(
			Integer transactionId, Transaction transaction, Integer componentId
	)
	{
		assert transaction != null;
		assert componentId != null;
		TransactionComponent transactionComponent = transaction.transactionComponents.get(
				componentId
		);
		assert transactionComponent != null;
		Component component = proxyStorage.getComponent(
				componentId
		);
		assert component != null;
		assert transactionComponent.componentPhase == ComponentPhase.NOT_UPDATED;
		if (
			!transactionId.equals(component.owner)
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
			component.newValue == null
		)
		{
			return new UnifiedAction(
					new ComponentChange(
							transactionId,
							componentId,
							component,
							new Component(
									component.version,
									component.owner,
									component.value,
									this.getNewValueForComponent(
											transactionId,
											transaction,
											componentId
									)
							)
					)
			);
		}
		else
		{
			logger.trace("TransactionAnalyzerImpl updating action impossible, downloading, transactionId: " + transactionId + " componentId: " + componentId);
			return new UnifiedAction(
					transactionId,
					transaction,
					componentId,
					new TransactionComponent(
							transactionComponent.versionToLock,
							ComponentPhase.UPDATED
					)
			);
		}
	}

	private Integer getNewValueForComponent(
			Integer transactionId, Transaction transaction, Integer componentId
	)
	{
		Function function = transaction.function;
		Map<Integer, Integer> functionArguments = new TreeMap<Integer, Integer>();
		for (SortedMap.Entry<Integer, TransactionComponent> transactionComponentEntry : transaction.transactionComponents.entrySet())
		{
			ComponentPhase componentPhase = transactionComponentEntry.getValue().componentPhase;
			assert componentPhase == ComponentPhase.NOT_UPDATED || componentPhase == ComponentPhase.UPDATED;
			Component component = this.proxyStorage.getComponent(
					transactionComponentEntry.getKey()
			);
			assert component != null;
			functionArguments.put(
					transactionComponentEntry.getKey(),
					component.value
			);
		}
		Map<Integer, Integer> functionResult = function.execute(
				functionArguments
		);
		Integer result = functionResult.get(
				componentId
		);
		assert result != null;
		return result;
	}

	private UnifiedAction getComponentReleasingAction(
			Integer transactionId, Transaction transaction, Integer componentId
	)
	{
		assert transaction != null;
		assert componentId != null;
		TransactionComponent transactionComponent = transaction.transactionComponents.get(
				componentId
		);
		assert transactionComponent != null;
		Component component = proxyStorage.getComponent(
				componentId
		);
		assert component != null;
		assert transactionComponent.componentPhase == ComponentPhase.NOT_RELEASED;
		if (
			transactionId.equals(component.owner)
			&&
			component.newValue != null
		)
		{
			assert component.version == transactionComponent.versionToLock;
			return new UnifiedAction(
					new ComponentChange(
							transactionId,
							componentId,
							component,
							new Component(
									component.version + 1,
									null,
									component.newValue,
									null
							)
					)
			);
		}
		else if (
			component.version > transactionComponent.versionToLock
		)
		{
			return new UnifiedAction(
					transactionId,
					transaction,
					componentId,
					new TransactionComponent(
							transactionComponent.versionToLock,
							ComponentPhase.RELEASED
					)
			);
		}
		else
		{
			logger.trace("TransactionAnalyzerImpl releasing action impossible, downloading, transactionId: " + transactionId + " componentId: " + componentId);
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
