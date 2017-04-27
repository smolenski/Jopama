package pl.rodia.jopama.integration.inmemory;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

import pl.rodia.jopama.integration.UniversalStorageAccess;

public class NonConfPerfTest
{
	@Test
	public void nonConflictingTransactionsPerformance() throws InterruptedException, ExecutionException
	{
		InMemoryStorageGateway inMemoryStorageGateway = new InMemoryStorageGateway();
		UniversalStorageAccess storageAccess = new InMemoryUniversalStorageAccess(
				inMemoryStorageGateway
		);
		pl.rodia.jopama.integration.NonConfPerfTest test = new pl.rodia.jopama.integration.NonConfPerfTest();
		test.nonConflictingTransactionsPerformance(
				storageAccess,
				inMemoryStorageGateway
		);
	}
}
