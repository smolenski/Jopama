package pl.rodia.jopama.core;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.Transaction;

public abstract class LocalStorage {
	abstract public Component getComponent(Integer componentId);
	abstract public Transaction getTransaction(Integer transactionId);	
	abstract public Boolean putComponent(Integer componentId, Component component);
	abstract public Boolean putTransaction(Integer transactionId, Transaction transaction);
}
