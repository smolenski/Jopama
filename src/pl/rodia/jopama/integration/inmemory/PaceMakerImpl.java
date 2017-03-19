package pl.rodia.jopama.integration.inmemory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.core.TransactionProcessor;
import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class PaceMakerImpl implements PaceMaker
{

	public PaceMakerImpl(
			String name,
			List<Integer> toDoTransactions,
			Integer numRunningPace,
			TransactionProcessor transactionProcessor,
			TaskRunner transactionTaskRunner
	)
	{
		super();
		this.finish = new Boolean(
				false
		);
		this.finished = new Boolean(
				false
		);
		this.taskRunner = new TaskRunner(
				name
		);
		this.taskRunnerThread = new Thread(
				this.taskRunner
		);
		this.numFinished = new Integer(0);
		this.name = name;
		this.waitingTransactions = new LinkedList<Integer>();
		for (Integer transactionId : toDoTransactions)
		{
			this.waitingTransactions.add(
					transactionId
			);
		}
		this.runningTransactions = new HashSet<Integer>();
		this.numRunningPace = numRunningPace;
		this.transactionProcessor = transactionProcessor;
		this.transactionTaskRunner = transactionTaskRunner;
	}

	public void start()
	{
		this.taskRunnerThread.start();
		
			this.taskRunner.schedule(
					new Task()
					{

						@Override
						public void execute()
						{
							scheduleUpToTheLimit();
						}
					}
			);
		
	}

	void teardown() throws InterruptedException
	{
		this.taskRunner.finish();
		this.taskRunnerThread.join();
	}
	
	public void prepareToFinish() throws InterruptedException, ExecutionException
	{
		CompletableFuture<Boolean> done = new CompletableFuture<Boolean>();
		this.taskRunner.schedule(new Task()
		{
			@Override
			public void execute()
			{
				finish = new Boolean(true);
				done.complete(new Boolean(true));
			}
		});
		Boolean result = done.get();
		assert result.equals(new Boolean(true));
	}

	public void finish() throws InterruptedException, ExecutionException
	{
		while (
			true
		)
		{
			CompletableFuture<Boolean> futureEmptyTransactions = new CompletableFuture<>();
			this.taskRunner.schedule(
					new Task()
					{

						@Override
						public void execute()
						{
							futureEmptyTransactions.complete(
									new Boolean(
											runningTransactions.isEmpty()
									)
							);
						}
					}
			);
			if (
				futureEmptyTransactions.get().equals(
						new Boolean(
								true
						)
				)
			)
			{
				break;
			}
			Thread.sleep(100);
		}
		this.teardown();
		this.finished = new Boolean(true);
	}

	void schedule(
			Integer transactionId
	)
	{
		logger.debug(
				this.name + "PaceMaker::scheduling: " + transactionId
		);
		this.transactionTaskRunner.schedule(
				new Task()
				{

					@Override
					public void execute()
					{
						transactionProcessor.addTransaction(
								transactionId,
								new Task()
								{
									@Override
									public void execute()
									{
										taskRunner.schedule(
												new Task()
												{
													@Override
													public void execute()
													{
														onTransactionDone(
																transactionId
														);
													}
												}
										);
									}
								}
						);

					}
				}
		);
	}

	void scheduleUpToTheLimit()
	{
		while (
			this.waitingTransactions.size() > 0 && this.runningTransactions.size() < this.numRunningPace
		)
		{
			Integer transactionId = this.waitingTransactions.remove(
					0
			);
			this.runningTransactions.add(
					transactionId
			);
			this.schedule(
					transactionId
			);
		}
	}

	void onTransactionDone(
			Integer transactionId
	)
	{
		logger.debug(
				this.name + "PaceMaker::onTransactionDone: " + transactionId
		);
		assert this.runningTransactions.contains(
				transactionId
		);
		this.runningTransactions.remove(
				transactionId
		);
		++this.numFinished;
		if (
			this.finish.equals(
					new Boolean(
							false
					)
			)
		)
		{
			this.scheduleUpToTheLimit();
		}
	}

	@Override
	public Integer getNumFinished()
	{
		assert this.finished.equals(new Boolean(true));
		return this.numFinished;
	}
	
	Boolean finish;
	Boolean finished;
	TaskRunner taskRunner;
	Thread taskRunnerThread;
	Integer numFinished;
	final String name;
	List<Integer> waitingTransactions;
	Set<Integer> runningTransactions;
	final Integer numRunningPace;
	TransactionProcessor transactionProcessor;
	TaskRunner transactionTaskRunner;
	static final Logger logger = LogManager.getLogger();

}
