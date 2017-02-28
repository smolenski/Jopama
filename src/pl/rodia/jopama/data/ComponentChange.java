package pl.rodia.jopama.data;

public class ComponentChange {

	public ComponentChange(Integer transactionId, Integer componentId, Component currentVersion, Component newVersion) {
		super();
		this.transactionId = transactionId;
		this.componentId = componentId;
		this.currentVersion = currentVersion;
		this.nextVersion = newVersion;
	}

	public Integer transactionId;
	public Integer componentId;
	public Component currentVersion;
	public Component nextVersion;
}
