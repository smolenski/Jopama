package pl.rodia.jopama.core;

import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.ObjectId;

public abstract class LocalStorage {
	abstract public ExtendedComponent getComponent(ObjectId componentId);
	abstract public ExtendedTransaction getTransaction(ObjectId transactionId);	
	abstract public Boolean putComponent(ObjectId componentId, ExtendedComponent extendedComponent);
	abstract public Boolean putTransaction(ObjectId transactionId, ExtendedTransaction extendedTransaction);
}
