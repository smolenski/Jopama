package pl.rodia.jopama.integration.inmemory;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.TransactionChange;
import pl.rodia.jopama.gateway.ErrorCode;
import pl.rodia.jopama.gateway.NewComponentVersionFeedback;
import pl.rodia.jopama.gateway.NewTransactionVersionFeedback;
import pl.rodia.jopama.gateway.RemoteStorageGateway;

public class InMemoryStorageGateway extends RemoteStorageGateway
{

	public InMemoryStorageGateway()
	{
		this.transactions = new HashMap<ObjectId, ExtendedTransaction>();
		this.components = new HashMap<ObjectId, ExtendedComponent>();
	}

	@Override
	synchronized public void requestTransaction(
			ObjectId transactionId, NewTransactionVersionFeedback feedback
	)
	{
		if (
			this.transactions.containsKey(
					transactionId
			)
		)
		{
			feedback.success(
					this.transactions.get(
							transactionId
					)
			);
		}
		else
		{
			feedback.failure(
					ErrorCode.NOT_EXISTS
			);
		}
	}

	@Override
	synchronized public void requestComponent(
			ObjectId componentId, NewComponentVersionFeedback feedback
	)
	{
		if (
			this.components.containsKey(
					componentId
			)
		)
		{
			feedback.success(
					this.components.get(
							componentId
					)
			);
		}
		else
		{
			feedback.failure(
					ErrorCode.NOT_EXISTS
			);
		}
	}

	@Override
	synchronized public void changeTransaction(
			TransactionChange transactionChange, NewTransactionVersionFeedback feedback
	)
	{
		if (
			this.transactions.containsKey(
					transactionChange.transactionId
			)
		)
		{
			ExtendedTransaction extendedTransaction = this.transactions.get(
					transactionChange.transactionId
			);
			if (
				transactionChange.currentVersion.equals(
						extendedTransaction
				)
			)
			{
				assert transactionChange.currentVersion.externalVersion.equals(
						extendedTransaction.externalVersion
				);
				if (
					transactionChange.nextVersion != null
				)
				{
					logger.debug(
							"InMemoryStorageGateway::changeTransaction SUCCESS"
					);
					ExtendedTransaction updatedExtendedTransaction = new ExtendedTransaction(
							transactionChange.nextVersion,
							new Integer(
									transactionChange.currentVersion.externalVersion + 1
							)
					);
					this.transactions.put(
							transactionChange.transactionId,
							updatedExtendedTransaction
					);
					feedback.success(
							updatedExtendedTransaction
					);
				}
				else
				{
					logger.debug(
							"InMemoryStorageGateway::changeTransaction (remove) SUCCESS"
					);
					this.transactions.remove(
							transactionChange.transactionId
					);
					feedback.success(
							null
					);
				}
			}
			else
			{
				feedback.failure(
						ErrorCode.BASE_VERSION_NOT_EQUAL
				);
			}
		}
		else
		{
			feedback.failure(
					ErrorCode.NOT_EXISTS
			);
		}
	}

	@Override
	synchronized public void changeComponent(
			ComponentChange componentChange, NewComponentVersionFeedback feedback
	)
	{
		if (
			this.components.containsKey(
					componentChange.componentId
			)
		)
		{
			ExtendedComponent extendedComponent = this.components.get(
					componentChange.componentId
			);
			if (
				componentChange.currentVersion.equals(
						extendedComponent
				)
			)
			{
				assert componentChange.currentVersion.externalVersion.equals(
						extendedComponent.externalVersion
				);
				if (
					componentChange.nextVersion != null
				)
				{
					logger.debug(
							"InMemoryStorageGateway::changeComponent SUCCESS"
					);
					ExtendedComponent updatedExtendedComponent = new ExtendedComponent(
							componentChange.nextVersion,
							new Integer(
									componentChange.currentVersion.externalVersion + 1
							)
					);
					this.components.put(
							componentChange.componentId,
							updatedExtendedComponent
					);
					feedback.success(
							updatedExtendedComponent
					);
				}
				else
				{
					logger.debug(
							"InMemoryStorageGateway::changeComponent (remove) SUCCESS"
					);
					this.components.remove(
							componentChange.componentId
					);
					feedback.success(
							null
					);
				}
			}
			else
			{
				feedback.failure(
						ErrorCode.BASE_VERSION_NOT_EQUAL
				);
			}
		}
		else
		{
			feedback.failure(
					ErrorCode.NOT_EXISTS
			);
		}
	}

	Map<ObjectId, ExtendedTransaction> transactions;
	Map<ObjectId, ExtendedComponent> components;
	static final Logger logger = LogManager.getLogger();

}
