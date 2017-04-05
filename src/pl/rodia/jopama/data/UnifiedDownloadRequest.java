package pl.rodia.jopama.data;

public class UnifiedDownloadRequest
{
	public UnifiedDownloadRequest(
			ObjectId transactionId
	)
	{
		this.transactionId = transactionId;
		this.componentId = null;
	}
	
	public UnifiedDownloadRequest(
			ObjectId transactionId, ObjectId componentId
	)
	{
		this.transactionId = transactionId;
		this.componentId = componentId;
	}

	public ObjectId transactionId;
	public ObjectId componentId;
}
