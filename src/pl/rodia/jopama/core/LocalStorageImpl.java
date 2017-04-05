package pl.rodia.jopama.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.ObjectId;

public class LocalStorageImpl extends LocalStorage
{

	public LocalStorageImpl()
	{
		this.transactions = new HashMap<ObjectId, ExtendedTransaction>();
		this.components = new HashMap<ObjectId, ExtendedComponent>();
	}

	@Override
	public ExtendedTransaction getTransaction(
			ObjectId transactionId
	)
	{
		return this.transactions.get(
				transactionId
		);
	}

	@Override
	public ExtendedComponent getComponent(
			ObjectId componentId
	)
	{
		return this.components.get(
				componentId
		);
	}

	public Boolean putComponent(
			ObjectId componentId, ExtendedComponent extendedComponent
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
			ObjectId transactionId, ExtendedTransaction extendedTransaction
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

	Map<ObjectId, ExtendedTransaction> transactions;
	Map<ObjectId, ExtendedComponent> components;
	static final Logger logger = LogManager.getLogger();
}
