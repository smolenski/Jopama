package pl.rodia.jopama.old;

import java.io.Serializable;

import pl.rodia.jopama.integration.zookeeper.ZooKeeperGroup;

public class OldZooKeeperObjectId implements Serializable
{
	public OldZooKeeperObjectId(
			Integer clusterId, ZooKeeperGroup groupId, Long objectId
	)
	{
		super();
		this.clusterId = clusterId;
		this.group = groupId;
		this.objectId = objectId;
	}

	public OldZooKeeperObjectId(
			Long id
	)
	{
		assert (id >> 60) == OldZooKeeperObjectId.magicValue;
		id = new Long(
				(id << 4) >> 4
		);
		this.clusterId = new Long(
				id >> 45
		).intValue();
		id = new Long(
				(id << 19) >> 19
		);
		System.out.println(
				"group: " + (id >> 40)
		);
		this.group = ZooKeeperGroup.values()[new Long(
				id >> 40
		).intValue()];
		id = new Long(
				(id << 24) >> 24
		);
		this.objectId = id;
	}

	public Long toLong()
	{
		assert (this.clusterId >> 15) == 0;
		assert (this.group.ordinal() >> 5) == 0;
		assert (this.objectId >> 40) == 0;
		return new Long(
				(new Long(
						OldZooKeeperObjectId.magicValue
				) << 60) | (new Long(
						this.clusterId
				) << 45) | (new Long(
						this.group.ordinal()
				) << 40) | this.objectId
		);
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
		OldZooKeeperObjectId other = (OldZooKeeperObjectId) obj;
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

	@Override
	public String toString()
	{
		return "(clusterId: " + this.clusterId + " group: " + this.group.name() + "objectId: " + this.objectId + ")";
	}

	Integer clusterId;
	ZooKeeperGroup group;
	Long objectId;
	static final Integer magicValue = 9;
	private static final long serialVersionUID = -1667024543558371506L;
}
