package pl.rodia.jopama.integration;

import java.util.LinkedList;
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
import pl.rodia.jopama.gateway.RemoteStorageGateway;
import pl.rodia.jopama.stats.StatsAsyncSource;
import pl.rodia.mpf.TaskRunner;

public class Integrator
{

	public Integrator(
			String name,
			RemoteStorageGateway targetRemoteStorageGateway,
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
		this.targetRemoteStorageGateway = targetRemoteStorageGateway;
		this.remoteStorageGatewayWrapper = new RemoteStorageGatewayImpl(
				this.taskRunner,
				this.targetRemoteStorageGateway
		);
		this.localStorage = new LocalStorageImpl();
		this.transactionAnalyzer = new TransactionAnalyzerImpl(
				this.localStorage
		);
		this.transactionProcessor = new TransactionProcessorImpl(
				this.taskRunner,
				this.transactionAnalyzer,
				this.localStorage,
				this.remoteStorageGatewayWrapper
		);
		this.paceMaker = new PaceMakerImpl(
				name + ":PM",
				toDoTransactions,
				numRunningPace,
				transactionProcessor,
				taskRunner
		);
	}

	public void start()
	{
		logger.info("Integrator start");
		this.taskRunnerThread.start();
		this.paceMaker.start();
		logger.info("Integrator start done");
	}

	public void prepareToFinish() throws InterruptedException, ExecutionException
	{
		logger.info("Integrator prepareToFinish");
		this.paceMaker.prepareToFinish();
	}
	
	public void finish() throws InterruptedException, ExecutionException
	{
		logger.info("Integrator finish");
		this.paceMaker.finish();
		this.taskRunner.finish();
		this.taskRunnerThread.join();
		logger.info("Integrator finish done");
	}
	
	public List<StatsAsyncSource> getStatsSources()
	{
		List<StatsAsyncSource> statsSources = new LinkedList<StatsAsyncSource>();
		StatsAsyncSource taskRunnerStatsSource = new StatsAsyncSource(
				this.taskRunner,
				this.taskRunner
		);
		statsSources.add(
				taskRunnerStatsSource
		);
		StatsAsyncSource remoteStorageGatewayStatsSource = new StatsAsyncSource(
				this.taskRunner,
				this.remoteStorageGatewayWrapper
		);
		statsSources.add(
				remoteStorageGatewayStatsSource
		);
		StatsAsyncSource transactionProcessorStatsSource = new StatsAsyncSource(
				this.taskRunner,
				this.transactionProcessor
		);
		statsSources.add(
				transactionProcessorStatsSource
		);
		return statsSources;
	}

	TaskRunner taskRunner;
	Thread taskRunnerThread;
	RemoteStorageGateway targetRemoteStorageGateway;
	RemoteStorageGatewayImpl remoteStorageGatewayWrapper;
	LocalStorage localStorage;
	TransactionAnalyzer transactionAnalyzer;
	TransactionProcessorImpl transactionProcessor;
	public PaceMaker paceMaker;
	static final Logger logger = LogManager.getLogger();
}
