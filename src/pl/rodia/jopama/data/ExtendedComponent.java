package pl.rodia.jopama.data;

public class ExtendedComponent
{
	public ExtendedComponent(
			Component component, Integer externalVersion
	)
	{
		super();
		this.component = component;
		this.externalVersion = externalVersion;
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((component == null) ? 0 : component.hashCode());
		result = prime * result + ((externalVersion == null) ? 0 : externalVersion.hashCode());
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
		ExtendedComponent other = (ExtendedComponent) obj;
		if (
			component == null
		)
		{
			if (
				other.component != null
			)
				return false;
		}
		else if (
			!component.equals(
					other.component
			)
		)
			return false;
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
		return true;
	}

	@Override
	public String toString()
	{
		return "ExtendedComponent [component=" + component + ", externalVersion=" + externalVersion + "]";
	}

	public Component component;
	public Integer externalVersion;
}
