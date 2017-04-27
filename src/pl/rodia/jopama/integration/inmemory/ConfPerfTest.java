package pl.rodia.jopama.integration.inmemory;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

import pl.rodia.jopama.integration.UniversalStorageAccess;

public class ConfPerfTest
{
	@Test
	public void conflictingTransactionsPerformance() throws InterruptedException, ExecutionException
	{
		InMemoryStorageGateway inMemoryStorageGateway = new InMemoryStorageGateway();
		UniversalStorageAccess storageAccess = new InMemoryUniversalStorageAccess(
				inMemoryStorageGateway
		);
		pl.rodia.jopama.integration.ConfPerfTest test = new pl.rodia.jopama.integration.ConfPerfTest();
		test.conflictingTransactionsPerformance(
				storageAccess,
				inMemoryStorageGateway
		);
	}
}
