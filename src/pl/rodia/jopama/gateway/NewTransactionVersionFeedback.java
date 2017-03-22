package pl.rodia.jopama.gateway;

import pl.rodia.jopama.data.ExtendedTransaction;

public interface NewTransactionVersionFeedback
{
	public void success(ExtendedTransaction extendedTransaction);
	public void failure(ErrorCode errorCode);
}
