package pl.rodia.jopama.integration;

import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.ObjectId;

public abstract class UniversalStorageAccess
{
	public abstract ObjectId createComponent(
			Long id, ExtendedComponent extendedComponent
	);

	public abstract ObjectId createTransaction(
			Long id, ExtendedTransaction extendedTransaction
	);

	public abstract ExtendedComponent getComponent(
			ObjectId objectId
	);

	public abstract ExtendedTransaction getTransaction(
			ObjectId objectId
	);
}
