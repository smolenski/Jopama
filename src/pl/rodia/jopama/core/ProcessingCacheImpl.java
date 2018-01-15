package pl.rodia.jopama.core;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.ObjectId;

public class ProcessingCacheImpl extends ProcessingCache
{

	public ProcessingCacheImpl()
	{
		this.caches = new HashMap<ObjectId, LocalStorage>();
	}
	
	@Override
	public void add(
			ObjectId transactionId
	)
	{
		logger.debug("Adding local storage: " + transactionId);
		assert this.caches.get(transactionId) == null;
		this.caches.put(transactionId, new LocalStorageImpl());
	}

	@Override
	public LocalStorage get(
			ObjectId transactionId
	)
	{
		LocalStorage result = this.caches.get(transactionId);
		assert result != null;
		return result;
	}

	@Override
	public void remove(
			ObjectId transactionId
	)
	{
		logger.debug("Removing local storage: " + transactionId);
		LocalStorage result = this.caches.remove(transactionId);
		assert result != null;
	}

	Map<ObjectId, LocalStorage> caches;
	static final Logger logger = LogManager.getLogger();
	
}
