package pl.rodia.jopama.integration.zookeeper;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

public class BasicIntegrationTest
{
	@Test
	public void singleTransactionConcurrentlyProcessedByManyIntegratorsZooKeeperTest() throws InterruptedException, ExecutionException
	{
		String addresses = "127.0.0.1:2181";
		Integer clusterSize = new Integer(
				1
		);
		ZooKeeperMultiProvider zooKeeperMultiProvider = new ZooKeeperMultiProvider(
				addresses,
				clusterSize
		);
		zooKeeperMultiProvider.start();
		ZooKeeperStorageAccess zooKeeperStorageAccess = new ZooKeeperStorageAccess(
				zooKeeperMultiProvider
		);
		ZooKeeperUniversalStorageAccess zooKeeperUniversalStorageAccess = new ZooKeeperUniversalStorageAccess(
				zooKeeperStorageAccess
		);
		ZooKeeperStorageGateway zooKeeperStorageGateway = new ZooKeeperStorageGateway(
				zooKeeperMultiProvider
		);
		pl.rodia.jopama.integration.BasicIntegrationTest test = new pl.rodia.jopama.integration.BasicIntegrationTest();
		test.singleTransactionConcurrentlyProcessedByManyIntegratorsTest(
				zooKeeperUniversalStorageAccess,
				zooKeeperStorageGateway
		);
		zooKeeperMultiProvider.finish();
	}
}
