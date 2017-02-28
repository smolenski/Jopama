package pl.rodia.jopama.data;

public class TransactionChange {

	public TransactionChange(Integer transactionId, Transaction currentVersion, Transaction newVersion) {
		super();
		this.transactionId = transactionId;
		this.currentVersion = currentVersion;
		this.nextVersion = newVersion;
	}

	public Integer transactionId;
	public Transaction currentVersion;
	public Transaction nextVersion;
}
