package pl.rodia.jopama.data;

public class TransactionComponent
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
	public String toString()
	{
		return "(" + "versionToLock: " + this.versionToLock + ", componentPhase: " + this.componentPhase + ")";
	}

	public Integer versionToLock;
	public ComponentPhase componentPhase;
}
