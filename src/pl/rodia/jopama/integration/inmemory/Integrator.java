package pl.rodia.jopama.integration.inmemory;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.core.LocalStorage;
import pl.rodia.jopama.core.LocalStorageImpl;
import pl.rodia.jopama.core.RemoteStorageGatewayImpl;
import pl.rodia.jopama.core.TransactionAnalyzer;
import pl.rodia.jopama.core.TransactionAnalyzerImpl;
import pl.rodia.jopama.core.TransactionProcessor;
import pl.rodia.jopama.core.TransactionProcessorImpl;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.mpf.TaskRunner;

public class Integrator
{

	public Integrator(
			String name,
			InMemoryStorageGateway inMemoryStorageGateway,
			List<ObjectId> toDoTransactions,
			Integer numRunningPace
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
		this.paceMaker = new PaceMakerImpl(
				name + ":PM",
				toDoTransactions,
				numRunningPace,
				transactionProcessor,
				taskRunner
		);
	}

	void start()
	{
		this.taskRunnerThread.start();
		this.paceMaker.start();
	}

	void prepareToFinish() throws InterruptedException, ExecutionException
	{
		this.paceMaker.prepareToFinish();
	}
	
	void finish() throws InterruptedException, ExecutionException
	{
		this.paceMaker.finish();
		this.taskRunner.finish();
		this.taskRunnerThread.join();
	}

	TaskRunner taskRunner;
	Thread taskRunnerThread;
	InMemoryStorageGateway inMemoryStorageGateway;
	RemoteStorageGatewayImpl remoteStorageGateway;
	LocalStorage localStorage;
	TransactionAnalyzer transactionAnalyzer;
	TransactionProcessor transactionProcessor;
	PaceMaker paceMaker;
	static final Logger logger = LogManager.getLogger();
}
