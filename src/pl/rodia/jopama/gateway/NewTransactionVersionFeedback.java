package pl.rodia.jopama.gateway;

import pl.rodia.jopama.data.Transaction;

public interface NewTransactionVersionFeedback
{
	public void success(Transaction transaction);
	public void failure(ErrorCode errorCode);
}
