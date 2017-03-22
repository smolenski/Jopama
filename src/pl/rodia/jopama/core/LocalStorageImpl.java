package pl.rodia.jopama.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;

public class LocalStorageImpl extends LocalStorage
{

	public LocalStorageImpl()
	{
		this.transactions = new HashMap<Integer, ExtendedTransaction>();
		this.components = new HashMap<Integer, ExtendedComponent>();
	}

	@Override
	public ExtendedTransaction getTransaction(
			Integer transactionId
	)
	{
		return this.transactions.get(
				transactionId
		);
	}

	@Override
	public ExtendedComponent getComponent(
			Integer componentId
	)
	{
		return this.components.get(
				componentId
		);
	}

	public Boolean putComponent(
			Integer componentId, ExtendedComponent extendedComponent
	)
	{
		assert extendedComponent != null;
		ExtendedComponent oldExtendedComponent = this.getComponent(
				componentId
		);
		this.components.put(
				componentId,
				extendedComponent
		);
		logger.info(
				"putComponent, updating(" + (!extendedComponent.equals(
						oldExtendedComponent
				)) + ") componentId: " + componentId + " component: " + extendedComponent
		);
		return !extendedComponent.equals(
				oldExtendedComponent
		);
	}

	public Boolean putTransaction(
			Integer transactionId, ExtendedTransaction extendedTransaction
	)
	{
		assert extendedTransaction != null;
		ExtendedTransaction oldExtendedTransaction = this.getTransaction(
				transactionId
		);
		this.transactions.put(
				transactionId,
				extendedTransaction
		);
		logger.info(
				"putTransaction, updating(" + (!extendedTransaction.equals(
						oldExtendedTransaction
				)) + ") transactionId: " + transactionId + " transaction: " + extendedTransaction
		);
		return !extendedTransaction.equals(
				oldExtendedTransaction
		);
	}

	Map<Integer, ExtendedTransaction> transactions;
	Map<Integer, ExtendedComponent> components;
	static final Logger logger = LogManager.getLogger();
}
