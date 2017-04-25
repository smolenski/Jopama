package pl.rodia.jopama.integration.inmemory;

import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.SimpleObjectId;
import pl.rodia.jopama.integration.UniversalStorageAccess;

public class InMemoryUniversalStorageAccess extends UniversalStorageAccess
{

	public InMemoryUniversalStorageAccess(
			InMemoryStorageGateway inMemoryStorageGateway
	)
	{
		super();
		this.inMemoryStorageGateway = inMemoryStorageGateway;
	}

	@Override
	public ObjectId createComponent(
			Long id, ExtendedComponent extendedComponent
	)
	{
		assert extendedComponent.externalVersion.equals(
				new Integer(
						0
				)
		);
		SimpleObjectId objectId = new SimpleObjectId(
				id
		);
		this.inMemoryStorageGateway.components.put(
				objectId,
				extendedComponent
		);
		return objectId;
	}

	@Override
	public ObjectId createTransaction(
			Long id, ExtendedTransaction extendedTransaction
	)
	{
		assert extendedTransaction.externalVersion.equals(
				new Integer(
						0
				)
		);
		SimpleObjectId objectId = new SimpleObjectId(
				id
		);
		this.inMemoryStorageGateway.transactions.put(
				objectId,
				extendedTransaction
		);
		return objectId;
	}

	InMemoryStorageGateway inMemoryStorageGateway;

	@Override
	public ExtendedComponent getComponent(
			ObjectId objectId
	)
	{
		return this.inMemoryStorageGateway.components.get(
				objectId
		);
	}

	@Override
	public ExtendedTransaction getTransaction(
			ObjectId objectId
	)
	{
		return this.inMemoryStorageGateway.transactions.get(
				objectId
		);
	}
}
