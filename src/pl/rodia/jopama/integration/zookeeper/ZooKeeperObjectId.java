package pl.rodia.jopama.integration.zookeeper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class ZooKeeperObjectId
{
	public ZooKeeperObjectId(
			Integer clusterId, ZooKeeperGroup groupId, Integer objectId
	)
	{
		super();
		this.clusterId = clusterId;
		this.group = groupId;
		this.objectId = objectId;
	}
	
	public ZooKeeperObjectId(
			Integer id
	)
	{
		assert (id >> 28) == ZooKeeperObjectId.magicValue;
		id = new Integer((id << 4) >> 4);
		this.clusterId = id >> 22;
		id = new Integer((id << 10) >> 10);
		this.group = ZooKeeperGroup.values()[(id >> 20)];
		id = new Integer((id << 12) >> 12);
		this.objectId = id;
	}
	
	public Integer toInteger()
	{
		assert (this.clusterId.intValue() >> 6) == 0;
		assert (this.group.ordinal() >> 2) == 0;
		assert (this.objectId.intValue() >> 20) == 0;
		return new Integer((ZooKeeperObjectId.magicValue << 28) | this.clusterId.intValue() << 22) | (this.group.ordinal() << 20) | (this.objectId.intValue());
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clusterId == null) ? 0 : clusterId.hashCode());
		result = prime * result + ((group == null) ? 0 : group.hashCode());
		result = prime * result + ((objectId == null) ? 0 : objectId.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj
	)
	{
		if (
			this == obj
		)
			return true;
		if (
			obj == null
		)
			return false;
		if (
			getClass() != obj.getClass()
		)
			return false;
		ZooKeeperObjectId other = (ZooKeeperObjectId) obj;
		if (
			clusterId == null
		)
		{
			if (
				other.clusterId != null
			)
				return false;
		}
		else if (
			!clusterId.equals(
					other.clusterId
			)
		)
			return false;
		if (
			group != other.group
		)
			return false;
		if (
			objectId == null
		)
		{
			if (
				other.objectId != null
			)
				return false;
		}
		else if (
			!objectId.equals(
					other.objectId
			)
		)
			return false;
		return true;
	}
	
	Integer clusterId;
	ZooKeeperGroup group;
	Integer objectId;
	static final Integer magicValue = 9;
}
