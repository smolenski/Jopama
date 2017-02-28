package pl.rodia.jopama.data;

import java.util.SortedMap;
import java.util.TreeMap;

public class Transaction
{

	public Transaction(
			TransactionPhase transactionPhase,
			TreeMap<Integer, TransactionComponent> transactionComponents,
			Function function
	)
	{
		super();
		this.transactionPhase = transactionPhase;
		this.transactionComponents = transactionComponents;
		this.function = function;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((transactionComponents == null) ? 0 : transactionComponents.hashCode());
		result = prime * result + ((transactionPhase == null) ? 0 : transactionPhase.hashCode());
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
		{
			return true;
		}
		if (
			obj == null
		)
		{
			return false;
		}
		if (
			!(obj instanceof Transaction)
		)
		{
			return false;
		}
		Transaction other = (Transaction) obj;
		if (
			transactionComponents == null
		)
		{
			if (
				other.transactionComponents != null
			)
			{
				return false;
			}
		}
		else if (
			!transactionComponents.equals(
					other.transactionComponents
			)
		)
		{
			return false;
		}
		if (
			transactionPhase != other.transactionPhase
		)
		{
			return false;
		}
		return true;
	}

	@Override
	public String toString()
	{
		StringBuilder result = new StringBuilder();
		result.append(
				"(" + "transactionPhase: " + this.transactionPhase
		);
		result.append(
				", transactionComponents: {"
		);
		for (SortedMap.Entry<Integer, TransactionComponent> transactionComponentEntry : this.transactionComponents.entrySet())
		{
			result.append(
					transactionComponentEntry.getKey() + " -> " + transactionComponentEntry.getValue()
			);
		}
		result.append(
				"})"
		);
		return result.toString();
	}

	public TransactionPhase transactionPhase;
	public TreeMap<Integer, TransactionComponent> transactionComponents;
	public final Function function;
}
