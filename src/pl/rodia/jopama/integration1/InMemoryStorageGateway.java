package pl.rodia.jopama.integration1;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.TransactionChange;
import pl.rodia.jopama.gateway.ErrorCode;
import pl.rodia.jopama.gateway.NewComponentVersionFeedback;
import pl.rodia.jopama.gateway.NewTransactionVersionFeedback;
import pl.rodia.jopama.gateway.RemoteStorageGateway;

public class InMemoryStorageGateway extends RemoteStorageGateway
{

	public InMemoryStorageGateway()
	{
		this.transactions = new HashMap<Integer, Transaction>();
		this.components = new HashMap<Integer, Component>();
	}

	@Override
	synchronized public void requestTransaction(
			Integer transactionId, NewTransactionVersionFeedback feedback
	)
	{
		if (this.transactions.containsKey(transactionId))
		{
			feedback.success(this.transactions.get(transactionId));
		}
		else
		{
			feedback.failure(ErrorCode.NOT_EXISTS);
		}
	}

	@Override
	synchronized public void requestComponent(
			Integer componentId, NewComponentVersionFeedback feedback
	)
	{
		if (this.components.containsKey(componentId))
		{
			feedback.success(this.components.get(componentId));
		}
		else
		{
			feedback.failure(ErrorCode.NOT_EXISTS);
		}
	}

	@Override
	synchronized public void changeTransaction(
			TransactionChange transactionChange, NewTransactionVersionFeedback feedback
	)
	{
		if (this.transactions.containsKey(transactionChange.transactionId))
		{
			Transaction transaction = this.transactions.get(transactionChange.transactionId);
			if (transactionChange.currentVersion.equals(transaction))
			{
				if (transactionChange.nextVersion != null)
				{
					logger.info("InMemoryStorageGateway::changeTransaction SUCCESS");
					this.transactions.put(transactionChange.transactionId, transactionChange.nextVersion);
					feedback.success(transactionChange.nextVersion);
				}
				else
				{
					logger.info("InMemoryStorageGateway::changeTransaction (remove) SUCCESS");
					this.transactions.remove(transactionChange.transactionId);
					feedback.success(null);
				}
			}
			else
			{
				feedback.failure(ErrorCode.BASE_VERSION_NOT_EQUAL);				
			}
		}
		else
		{
			feedback.failure(ErrorCode.NOT_EXISTS);
		}
	}

	@Override
	synchronized public void changeComponent(
			ComponentChange componentChange, NewComponentVersionFeedback feedback
	)
	{
		if (this.components.containsKey(componentChange.componentId))
		{
			Component component = this.components.get(componentChange.componentId);
			if (componentChange.currentVersion.equals(component))
			{
				if (componentChange.nextVersion != null)
				{
					logger.info("InMemoryStorageGateway::changeComponent SUCCESS");
					this.components.put(componentChange.componentId, componentChange.nextVersion);
					feedback.success(componentChange.nextVersion);
				}
				else
				{
					logger.info("InMemoryStorageGateway::changeComponent (remove) SUCCESS");
					this.components.remove(componentChange.componentId);
					feedback.success(null);
				}
			}
			else
			{
				feedback.failure(ErrorCode.BASE_VERSION_NOT_EQUAL);				
			}
		}
		else
		{
			feedback.failure(ErrorCode.NOT_EXISTS);
		}
	}

	Map<Integer, Transaction> transactions;
	Map<Integer, Component> components;
	static final Logger logger = LogManager.getLogger();

}
