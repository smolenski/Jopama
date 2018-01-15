package pl.rodia.jopama.core;

import pl.rodia.jopama.data.ObjectId;

public abstract class ProcessingCache
{
	abstract public void add(ObjectId transactionId);
	abstract public LocalStorage get(ObjectId transactionId);
	abstract public void remove(ObjectId transactionId);
}
