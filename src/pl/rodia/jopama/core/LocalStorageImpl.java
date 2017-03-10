package pl.rodia.jopama.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.Transaction;

public class LocalStorageImpl extends LocalStorage
{

	public LocalStorageImpl(
	)
	{
		this.transactions = new HashMap<Integer, Transaction>();
		this.components = new HashMap<Integer, Component>();
	}

	@Override
	public Transaction getTransaction(
			Integer transactionId
	)
	{
		return this.transactions.get(
				transactionId
		);
	}

	@Override
	public Component getComponent(
			Integer componentId
	)
	{
		return this.components.get(
				componentId
		);
	}

	public Boolean putComponent(
			Integer componentId, Component component
	)
	{
		assert component != null;
		Component oldComponent = this.getComponent(componentId);
		this.components.put(
				componentId,
				component
		);
		logger.info("putComponent, updating(" + (!component.equals(oldComponent)) + ") componentId: " + componentId + " component: " + component);
		return !component.equals(oldComponent);
	}

	public Boolean putTransaction(
			Integer transactionId, Transaction transaction
	)
	{
		assert transaction != null;
		Transaction oldTransaction = this.getTransaction(transactionId);
		this.transactions.put(
				transactionId,
				transaction
		);
		logger.info("putTransaction, updating(" + (!transaction.equals(oldTransaction)) + ") transactionId: " + transactionId +" transaction: " + transaction);
		return !transaction.equals(oldTransaction);
	}

	Map<Integer, Transaction> transactions;
	Map<Integer, Component> components;
	static final Logger logger = LogManager.getLogger();
}
