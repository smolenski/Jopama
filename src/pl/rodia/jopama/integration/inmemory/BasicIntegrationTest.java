package pl.rodia.jopama.integration.inmemory;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

import pl.rodia.jopama.integration.UniversalStorageAccess;

public class BasicIntegrationTest
{
	@Test
	public void singleTransactionConcurrentlyProcessedByManyIntegratorsInMemoryTest(
	) throws InterruptedException, ExecutionException
	{
		InMemoryStorageGateway inMemoryStorageGateway = new InMemoryStorageGateway();
		UniversalStorageAccess storageAccess = new InMemoryUniversalStorageAccess(inMemoryStorageGateway);
		pl.rodia.jopama.integration.BasicIntegrationTest test = new pl.rodia.jopama.integration.BasicIntegrationTest();
		test.singleTransactionConcurrentlyProcessedByManyIntegratorsTest(
				storageAccess,
				inMemoryStorageGateway
		);
	}

}
