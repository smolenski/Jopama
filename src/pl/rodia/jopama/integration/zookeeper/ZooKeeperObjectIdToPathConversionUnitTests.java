package pl.rodia.jopama.integration.zookeeper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class ZooKeeperObjectIdToPathConversionUnitTests
{
	@Test
	public void checkConversion()
	{
		ZooKeeperObjectId objectId = new ZooKeeperObjectId(
				"ABC"
		);
		String componentPath = ZooKeeperHelpers.getComponentPath(
				objectId
		);
		ZooKeeperObjectId componentObjectIdFromPath = ZooKeeperHelpers.getIdFromPath(
				componentPath
		);
		assertThat(
				componentObjectIdFromPath,
				equalTo(
						objectId
				)
		);
		String transactionPath = ZooKeeperHelpers.getTransactionPath(
				objectId
		);
		ZooKeeperObjectId transactionObjectIdFromPath = ZooKeeperHelpers.getIdFromPath(
				transactionPath
		);
		assertThat(
				transactionObjectIdFromPath,
				equalTo(
						objectId
				)
		);
	}
}
