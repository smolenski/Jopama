package pl.rodia.jopama.integration1;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.LocalStorage;
import pl.rodia.jopama.LocalStorageImpl;
import pl.rodia.jopama.RemoteStorageGatewayImpl;
import pl.rodia.jopama.TransactionAnalyzer;
import pl.rodia.jopama.TransactionAnalyzerImpl;
import pl.rodia.jopama.TransactionProcessor;
import pl.rodia.jopama.TransactionProcessorImpl;
import pl.rodia.jopama.gateway.RemoteStorageGateway;
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

	public void addTransaction(
			Integer transactionId
	)
	{
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

	public void waitUntilAllTransactionsProcessed() throws InterruptedException, ExecutionException
	{
		while (
			true
		)
		{
			CompletableFuture<Integer> numOparations = new CompletableFuture<Integer>();
			this.taskRunner.schedule(
					new Task()
					{
						@Override
						public void execute()
						{
							numOparations.complete(
									transactionProcessor.getNumTransactions()
							);
						}
					}
			);
			if (
				numOparations.get().equals(
						new Integer(
								0
						)
				)
			)
			{
				break;
			}
		}
	}

	TaskRunner taskRunner;
	Thread taskRunnerThread;
	InMemoryStorageGateway inMemoryStorageGateway;
	RemoteStorageGatewayImpl remoteStorageGateway;
	LocalStorage localStorage;
	TransactionAnalyzer transactionAnalyzer;
	TransactionProcessor transactionProcessor;
	static final Logger logger = LogManager.getLogger();
}
