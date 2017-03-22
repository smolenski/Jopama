package pl.rodia.jopama.data;

public class ExtendedTransaction
{
	public ExtendedTransaction(
			Transaction transaction, Integer externalVersion
	)
	{
		super();
		this.transaction = transaction;
		this.externalVersion = externalVersion;
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((externalVersion == null) ? 0 : externalVersion.hashCode());
		result = prime * result + ((transaction == null) ? 0 : transaction.hashCode());
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
		ExtendedTransaction other = (ExtendedTransaction) obj;
		if (
			externalVersion == null
		)
		{
			if (
				other.externalVersion != null
			)
				return false;
		}
		else if (
			!externalVersion.equals(
					other.externalVersion
			)
		)
			return false;
		if (
			transaction == null
		)
		{
			if (
				other.transaction != null
			)
				return false;
		}
		else if (
			!transaction.equals(
					other.transaction
			)
		)
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return "ExtendedTransaction [transaction=" + transaction + ", externalVersion=" + externalVersion + "]";
	}

	public Transaction transaction;
	public Integer externalVersion;
}
