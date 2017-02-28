package pl.rodia.jopama.old;

import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.TransactionChange;

public abstract class Gateway
{

	public abstract void requestTransaction(
			Integer transactionId
	);

	public abstract void requestComponent(
			Integer transactionId,
			Integer componentId
	);

	public abstract void changeTransaction(
			TransactionChange transactionChange
	);

	public abstract void changeComponent(
			ComponentChange componentChange
	);

}
