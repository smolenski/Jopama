package pl.rodia.jopama.core;

import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;

public abstract class LocalStorage {
	abstract public ExtendedComponent getComponent(Integer componentId);
	abstract public ExtendedTransaction getTransaction(Integer transactionId);	
	abstract public Boolean putComponent(Integer componentId, ExtendedComponent extendedComponent);
	abstract public Boolean putTransaction(Integer transactionId, ExtendedTransaction extendedTransaction);
}
