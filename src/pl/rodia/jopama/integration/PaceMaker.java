package pl.rodia.jopama.integration;

import java.util.SortedMap;
import java.util.concurrent.ExecutionException;

import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.Transaction;

public interface PaceMaker
{
	public void start();
	public void addTransactions(SortedMap<ObjectId, Transaction> transactionIds);
	public void prepareToFinish() throws InterruptedException, ExecutionException;
	public void finish() throws InterruptedException, ExecutionException;
	Integer getNumFinished();
}
