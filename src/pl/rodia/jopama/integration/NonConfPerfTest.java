package pl.rodia.jopama.integration;

import java.util.concurrent.ExecutionException;

import pl.rodia.jopama.gateway.RemoteStorageGateway;

public class NonConfPerfTest
{
	public void nonConflictingTransactionsPerformance(
			UniversalStorageAccess storageAccess,
			RemoteStorageGateway storageGateway
	) throws InterruptedException, ExecutionException
	{
		RandomExchangesIntegrationTest exchangesTester = new RandomExchangesIntegrationTest();
		exchangesTester.performTest(
				storageAccess,
				storageGateway,
				10,
				10000,
				10,
				1000,
				2,
				5,
				3,
				30
		);
	}
}
