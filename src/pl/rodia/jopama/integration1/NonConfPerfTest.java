package pl.rodia.jopama.integration1;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

public class NonConfPerfTest
{
	@Test
	public void nonConflictingTransactionsPerformance() throws InterruptedException, ExecutionException
	{
		RandomExchangesIntegrationTest exchangesTester = new RandomExchangesIntegrationTest();
		exchangesTester.performTest(
				10,
				10000,
				10,
				1000,
				2,
				5,
				30
		);
	}
}
