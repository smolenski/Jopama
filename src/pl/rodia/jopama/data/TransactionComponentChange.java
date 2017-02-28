package pl.rodia.jopama.data;

public class TransactionComponentChange {
	public TransactionComponentChange(TransactionComponent currentVersion, TransactionComponent newVersion) {
		super();
		this.currentVersion = currentVersion;
		this.newVersion = newVersion;
	}

	public TransactionComponent currentVersion;
	public TransactionComponent newVersion;
}
