package pl.rodia.jopama.data;

public class TransactionChange {

	public TransactionChange(Integer transactionId, ExtendedTransaction currentVersion, Transaction newVersion) {
		super();
		this.transactionId = transactionId;
		this.currentVersion = currentVersion;
		this.nextVersion = newVersion;
	}

	public Integer transactionId;
	public ExtendedTransaction currentVersion;
	public Transaction nextVersion;
}
