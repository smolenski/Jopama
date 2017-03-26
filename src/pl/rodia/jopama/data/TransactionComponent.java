package pl.rodia.jopama.data;

import java.io.Serializable;

public class TransactionComponent implements Serializable
{
	public TransactionComponent(
			Integer versionToLock,
			ComponentPhase componentPhase
	)
	{
		super();
		this.versionToLock = versionToLock;
		this.componentPhase = componentPhase;
	}
	
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((componentPhase == null) ? 0 : componentPhase.hashCode());
		result = prime * result + ((versionToLock == null) ? 0 : versionToLock.hashCode());
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
		TransactionComponent other = (TransactionComponent) obj;
		if (
			componentPhase != other.componentPhase
		)
			return false;
		if (
			versionToLock == null
		)
		{
			if (
				other.versionToLock != null
			)
				return false;
		}
		else if (
			!versionToLock.equals(
					other.versionToLock
			)
		)
			return false;
		return true;
	}



	@Override
	public String toString()
	{
		return "(" + "versionToLock: " + this.versionToLock + ", componentPhase: " + this.componentPhase + ")";
	}

	public Integer versionToLock;
	public ComponentPhase componentPhase;
	private static final long serialVersionUID = -4891416575426982259L;
}
