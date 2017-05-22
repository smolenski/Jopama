package pl.rodia.jopama.integration.zookeeper;

import java.io.Serializable;
import java.util.Random;

import pl.rodia.jopama.data.ObjectId;

public class ZooKeeperObjectId extends ObjectId implements Serializable
{
	public ZooKeeperObjectId(
			String uniqueName
	)
	{
		super();
		this.uniqueName = uniqueName;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.uniqueName == null) ? 0 : this.uniqueName.hashCode());
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
			this.uniqueName == null
		)
		{
			if (
				other.uniqueName != null
			)
				return false;
		}
		else if (
			!this.uniqueName.equals(
					other.uniqueName
			)
		)
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return "uniqueName: " + this.uniqueName;
	}

	@Override
	public int compareTo(
			ObjectId obj
	)
	{
		if (
			this == obj
		)
		{
			return 0;
		}
		if (
			obj == null
		)
		{
			return 1;
		}
		if (
			getClass() != obj.getClass()
		)
		{
			throw new IllegalStateException(
					"Comparison between different classes of objectIds is not supported"
			);
		}
		ZooKeeperObjectId other = (ZooKeeperObjectId) obj;
		return this.uniqueName.compareTo(
				other.uniqueName
		);
	}
	
	public Long getId()
	{
		assert this.uniqueName.indexOf('_') + 21 == this.uniqueName.length();
		String numStr = this.uniqueName.substring(this.uniqueName.length() - 21);
		Long numValue = Long.parseLong(numStr);
		return numValue;
	}
	
	public Integer getClusterId(Integer numClusters)
	{
		return getClusterId(this.getId(), numClusters);
	}
	
	static public Integer getClusterId(Long id, Integer numClusters)
	{
		Long clusterId = Math.floorMod(id, new Long(numClusters));
		assert clusterId <= Integer.MAX_VALUE;
		return new Integer(clusterId.intValue());
	}
	
	static public Long getRandomIdForCluster(Random random, Integer clusterId, Integer numClusters)
	{
		Long rid = random.nextLong();
		Long mod = Math.floorMod(rid, new Long(numClusters));
		Long id = rid + clusterId + numClusters - mod;
		assert getClusterId(id, numClusters).equals(new Integer(clusterId));
		return new Long(id);
	}
		
	static public String getComponentUniqueName(Long componentId)
	{
		return String.format("%s_%020d", componentPrefix, componentId);
	}
	
	static public String getTransactionUniqueName(Long transactionId)
	{
		return String.format("%s_%020d", transactionPrefix, transactionId);
	}
	
	String uniqueName;
	static String componentPrefix = "Component_";
	static String transactionPrefix = "Transaction_";
	private static final long serialVersionUID = -1667024543558371506L;
}
