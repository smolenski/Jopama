package pl.rodia.jopama.data;

public class ComponentChange {

	public ComponentChange(Integer transactionId, Integer componentId, ExtendedComponent currentVersion, Component newVersion) {
		super();
		this.transactionId = transactionId;
		this.componentId = componentId;
		this.currentVersion = currentVersion;
		this.nextVersion = newVersion;
	}

	public Integer transactionId;
	public Integer componentId;
	public ExtendedComponent currentVersion;
	public Component nextVersion;
}
