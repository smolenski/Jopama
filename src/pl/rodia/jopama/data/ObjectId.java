package pl.rodia.jopama.data;

import java.io.Serializable;

public abstract class ObjectId implements Serializable, Comparable<ObjectId>
{
	public abstract Long toLong();
	
	@Override
	public int compareTo(
			ObjectId o
	)
	{
		if (o == null)
		{
			throw new NullPointerException();
		}
		return this.toLong().compareTo(o.toLong());
	}
	
	private static final long serialVersionUID = -1406438078333659301L;
}
