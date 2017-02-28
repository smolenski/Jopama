package pl.rodia.jopama;

import java.util.HashMap;
import java.util.Map;

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

	public void putComponent(
			Integer componentId, Component component
	)
	{
		this.components.put(
				componentId,
				component
		);
	}

	public void putTransaction(
			Integer transactionId, Transaction transaction
	)
	{
		this.transactions.put(
				transactionId,
				transaction
		);
	}

	Map<Integer, Transaction> transactions;
	Map<Integer, Component> components;
}
