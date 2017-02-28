package pl.rodia.jopama.data;

public class UnifiedDownloadRequest
{
	public UnifiedDownloadRequest(
			Integer transactionId
	)
	{
		this.transactionId = transactionId;
		this.componentId = null;
	}
	
	public UnifiedDownloadRequest(
			Integer transactionId, Integer componentId
	)
	{
		this.transactionId = transactionId;
		this.componentId = componentId;
	}

	public Integer transactionId;
	public Integer componentId;
}
