package pl.rodia.jopama.integration.zookeeper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class ZooKeeperObjectIdUnitTests
{
	@Test
	public void checkConversion()
	{
		ZooKeeperObjectId objectId = new ZooKeeperObjectId(new Integer(11), ZooKeeperGroup.TRANSACTION, 5781);
		Integer value = objectId.toInteger();
		ZooKeeperObjectId objectIdFromIntegerRepresentation = new ZooKeeperObjectId(value);
		assertThat(objectIdFromIntegerRepresentation, equalTo(objectId));
	}
}
