package pl.rodia.jopama.integration.zookeeper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class ZooKeeperObjectIdToPathConversionUnitTests
{
	@Test
	public void checkConversion()
	{
		ZooKeeperObjectId objectId = new ZooKeeperObjectId("ABC");
		String path = ZooKeeperHelpers.getPath(objectId);
		ZooKeeperObjectId objectIdFromPath = ZooKeeperHelpers.getIdFromPath(path);
		assertThat(objectIdFromPath, equalTo(objectId));
	}
}
