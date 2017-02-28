package pl.rodia.jopama.gateway;

import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.TransactionChange;

public abstract class RemoteStorageGateway
{

	abstract public void requestTransaction(
			Integer transactionId,
			NewTransactionVersionFeedback feedback
	);

	abstract public void requestComponent(
			Integer componentId,
			NewComponentVersionFeedback feedback
	);

	abstract public void changeTransaction(
			TransactionChange transactionChange,
			NewTransactionVersionFeedback feedback
	);

	abstract public void changeComponent(
			ComponentChange componentChange,
			NewComponentVersionFeedback feedback
	);

}
