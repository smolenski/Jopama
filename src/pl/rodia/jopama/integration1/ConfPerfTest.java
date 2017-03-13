package pl.rodia.jopama.integration1;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

public class ConfPerfTest
{
	@Test
	public void conflictingTransactionsPerformance() throws InterruptedException, ExecutionException
	{
		RandomExchangesIntegrationTest exchangesTester = new RandomExchangesIntegrationTest();
		exchangesTester.performTest(
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
