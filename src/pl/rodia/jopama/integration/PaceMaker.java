package pl.rodia.jopama.integration;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import pl.rodia.jopama.data.ObjectId;

public interface PaceMaker
{
	public void start();
	public void addTransactions(Set<ObjectId> transactionIds);
	public void prepareToFinish() throws InterruptedException, ExecutionException;
	public void finish() throws InterruptedException, ExecutionException;
	Integer getNumFinished();
}
