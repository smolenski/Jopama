package pl.rodia.jopama.data;

import java.io.Serializable;

public class SimpleObjectId extends ObjectId implements Serializable
{
	public SimpleObjectId(
			int id
	)
	{
		super();
		this.id = new Integer(id);
	}
	
	public SimpleObjectId(
			Integer id
	)
	{
		super();
		this.id = id;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
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
		SimpleObjectId other = (SimpleObjectId) obj;
		if (
			id == null
		)
		{
			if (
				other.id != null
			)
				return false;
		}
		else if (
			!id.equals(
					other.id
			)
		)
			return false;
		return true;
	}

	@Override
	public Long toLong()
	{
		return new Long(this.id);
	}
	
	@Override
	public String toString()
	{
		return this.id.toString();
	}
	
	public Integer id;
	private static final long serialVersionUID = -1406438078333659301L;

}