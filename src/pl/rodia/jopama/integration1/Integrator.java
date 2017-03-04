package pl.rodia.jopama.integration1;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.LocalStorage;
import pl.rodia.jopama.LocalStorageImpl;
import pl.rodia.jopama.RemoteStorageGatewayImpl;
import pl.rodia.jopama.TransactionAnalyzer;
import pl.rodia.jopama.TransactionAnalyzerImpl;
import pl.rodia.jopama.TransactionProcessorImpl;
import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.Increment;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.TransactionComponent;
import pl.rodia.jopama.data.TransactionPhase;
import pl.rodia.jopama.gateway.ErrorCode;
import pl.rodia.jopama.gateway.NewTransactionVersionFeedback;
import pl.rodia.jopama.stats.StatsAsyncSource;
import pl.rodia.jopama.stats.StatsCollector;
import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class Integrator
{

	public Integrator(
			String name,
			InMemoryStorageGateway inMemoryStorageGateway
	)
	{
		this.taskRunner = new TaskRunner(
				name
		);
		this.taskRunnerThread = new Thread(
				this.taskRunner
		);
		this.inMemoryStorageGateway = inMemoryStorageGateway;
		this.remoteStorageGateway = new RemoteStorageGatewayImpl(
				this.taskRunner,
				this.inMemoryStorageGateway
		);
		this.localStorage = new LocalStorageImpl();
		this.transactionAnalyzer = new TransactionAnalyzerImpl(
				this.localStorage
		);
		this.transactionProcessor = new TransactionProcessorImpl(
				this.taskRunner,
				this.transactionAnalyzer,
				this.localStorage,
				this.remoteStorageGateway
		);
		this.pendingTransactions = new HashSet<Integer>();
	}

	void start()
	{
		this.taskRunnerThread.start();
	}

	void teardown() throws InterruptedException
	{
		this.taskRunner.finish();
		this.taskRunnerThread.join();
	}

	synchronized public void addTransaction(
			Integer transactionId
	)
	{
		this.pendingTransactions.add(
				transactionId
		);
		this.taskRunner.schedule(
				new Task()
				{

					@Override
					public void execute()
					{
						transactionProcessor.addTransaction(
								transactionId
						);
					}
				}
		);
	}

	synchronized public void checkForRemovedTransactions()
	{
		Set<Integer> pt = new HashSet<Integer>();
		for (Integer i : this.pendingTransactions)
		{
			pt.add(i);
		}
		for (Integer transactionId : pt)
		{
			this.inMemoryStorageGateway.requestTransaction(
					transactionId,
					new NewTransactionVersionFeedback()
					{

						@Override
						public void success(
								Transaction transaction
						)
						{
						}

						@Override
						public void failure(
								ErrorCode errorCode
						)
						{
							if (
								errorCode == ErrorCode.NOT_EXISTS
							)
							{
								onTransactionRemoved(
										transactionId
								);
							}
						}
					}
			);
		}
	}

	synchronized public void onTransactionRemoved(
			Integer transactionId
	)
	{
		this.pendingTransactions.remove(
				transactionId
		);
		this.notify();
	}

	synchronized public void waitUntilTransactionProcessingFinished() throws InterruptedException
	{
		while (
			true
		)
		{
			this.checkForRemovedTransactions();
			if (
				this.pendingTransactions.size() == 0
			)
			{
				break;
			}
			wait(
					1000
			);
		}
	}

	TaskRunner taskRunner;
	Thread taskRunnerThread;
	InMemoryStorageGateway inMemoryStorageGateway;
	RemoteStorageGatewayImpl remoteStorageGateway;
	LocalStorage localStorage;
	TransactionAnalyzer transactionAnalyzer;
	TransactionProcessorImpl transactionProcessor;
	Set<Integer> pendingTransactions;
	static final Logger logger = LogManager.getLogger();
}
