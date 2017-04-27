package pl.rodia.jopama.integration;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

import pl.rodia.jopama.gateway.RemoteStorageGateway;

public class ConfPerfTest
{
	public void conflictingTransactionsPerformance(
			UniversalStorageAccess storageAccess,
			RemoteStorageGateway storageGateway
	) throws InterruptedException, ExecutionException
	{
		RandomExchangesIntegrationTest exchangesTester = new RandomExchangesIntegrationTest();
		exchangesTester.performTest(
				storageAccess,
				storageGateway,
				10,
				100,
				10,
				1000,
				2,
				5,
				30
		);
	}
}
