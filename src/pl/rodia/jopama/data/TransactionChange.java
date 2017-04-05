package pl.rodia.jopama.data;

public class TransactionChange {

	public TransactionChange(ObjectId transactionId, ExtendedTransaction currentVersion, Transaction newVersion) {
		super();
		this.transactionId = transactionId;
		this.currentVersion = currentVersion;
		this.nextVersion = newVersion;
	}

	public ObjectId transactionId;
	public ExtendedTransaction currentVersion;
	public Transaction nextVersion;
}
