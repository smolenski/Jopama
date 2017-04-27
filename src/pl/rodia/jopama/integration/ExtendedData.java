package pl.rodia.jopama.integration;

public class ExtendedData
{
	public ExtendedData(
			byte[] data, Integer version
	)
	{
		super();
		this.data = data;
		this.version = version;
	}

	public byte[] data;
	public Integer version;
}
