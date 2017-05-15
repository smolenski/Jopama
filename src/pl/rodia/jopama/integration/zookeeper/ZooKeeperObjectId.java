package pl.rodia.jopama.integration.zookeeper;

import java.io.Serializable;

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
	
	public Integer getClusterId(Integer numClusters)
	{
		return new Integer(Math.floorMod(this.hashCode(), numClusters));
	}
	
	static public String getComponentUniqueName(Long componentId)
	{
		return String.format("Component_%020d", componentId); 
	}
	
	static public String getTransactionUniqueName(Long transactionId)
	{
		return String.format("Transaction_%020d", transactionId); 
	}
	
	String uniqueName;
	private static final long serialVersionUID = -1667024543558371506L;
}
