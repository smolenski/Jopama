package pl.rodia.jopama.data;

public class ComponentChange {

	public ComponentChange(ObjectId transactionId, ObjectId componentId, ExtendedComponent currentVersion, Component newVersion) {
		super();
		this.transactionId = transactionId;
		this.componentId = componentId;
		this.currentVersion = currentVersion;
		this.nextVersion = newVersion;
	}

	public ObjectId transactionId;
	public ObjectId componentId;
	public ExtendedComponent currentVersion;
	public Component nextVersion;
}
