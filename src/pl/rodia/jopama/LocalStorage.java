package pl.rodia.jopama;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.Transaction;

public abstract class LocalStorage {
	abstract public Component getComponent(Integer componentId);
	abstract public Transaction getTransaction(Integer transactionId);	
	abstract public void putComponent(Integer componentId, Component component);
	abstract public void putTransaction(Integer transactionId, Transaction transaction);
}