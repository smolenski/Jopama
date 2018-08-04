package pl.rodia.jopama.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.core.TransactionProcessor;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class PaceMakerImpl implements PaceMaker
{

	public PaceMakerImpl(
			String name,
			SortedMap<ObjectId, Transaction> toDoTransactions,
			Integer numRunningPace,
			Integer singleComponentLimit,
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
		this.numFinished = new Integer(
				0
		);
		this.name = name;
        this.lastCandidate = null;
		this.waitingTransactions = new TreeMap<ObjectId, Transaction>();
		this.waitingTransactions.putAll(toDoTransactions);
		this.runningTransactions = new HashMap<ObjectId, Transaction>();
		this.numRunningPace = numRunningPace;
		this.singleComponentLimit = singleComponentLimit;
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
		logger.info("PaceMakerImpl teardown");
		this.taskRunner.finish();
		this.taskRunnerThread.join();
	}

	public void prepareToFinish() throws InterruptedException, ExecutionException
	{
		logger.info("PaceMakerImpl prepareToFinish");
		CompletableFuture<Boolean> done = new CompletableFuture<Boolean>();
		this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						finish = new Boolean(
								true
						);
						done.complete(
								new Boolean(
										true
								)
						);
					}
				}
		);
		Boolean result = done.get();
		assert result.equals(
				new Boolean(
						true
				)
		);
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
			Thread.sleep(
					100
			);
		}
		this.teardown();
		this.finished = new Boolean(
				true
		);
	}

	public void addTransactions(
			SortedMap<ObjectId, Transaction> transactionIds
	)
	{
		if (
			this.finish.equals(
					new Boolean(
							true
					)
			)
		)
		{
			return;
		}
		this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						waitingTransactions.putAll(
								transactionIds
						);
						scheduleUpToTheLimit();
					}
				}
		);
	}

	void schedule(
			ObjectId transactionId
	)
	{
		logger.info(
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
		logger.info(
				this.name + "PaceMaker::scheduling done: " + transactionId
		);
	}
	
	void moveToNextEntry()
	{
		if (this.waitingTransactions.isEmpty())
		{
			this.lastCandidate = null;
		}
		else if (this.lastCandidate == null)
		{
			this.lastCandidate = this.waitingTransactions.firstKey();
		}
		else
		{
			this.lastCandidate = this.waitingTransactions.higherKey(this.lastCandidate);
			if (this.lastCandidate == null)
			{
				this.lastCandidate = this.waitingTransactions.firstKey();
			}
		}
	}
	
	ObjectId getNextTransactionIdToPass()
	{
		Map<Long, Long> compsCount = new HashMap<Long, Long>();
		for (Map.Entry<ObjectId, Transaction> entry : this.runningTransactions.entrySet())
		{
			ZooKeeperTransactionHelpers.updateCompCount(compsCount, ZooKeeperTransactionHelpers.getCompIds(entry.getValue()));
		}
		for (int i = 0; i < this.waitingTransactions.size(); ++i)
		{
			this.moveToNextEntry();
			assert (this.lastCandidate != null);
			if (ZooKeeperTransactionHelpers.allCompsBelowLimit(
				this.singleComponentLimit,
				compsCount,
				ZooKeeperTransactionHelpers.getCompIds(
					this.waitingTransactions.get(this.lastCandidate)
				)
			))
			{
				return this.lastCandidate;
			}
		}
		return null;
	}

	void scheduleUpToTheLimit()
	{
		if (
			this.finish.equals(
					new Boolean(
							true
					)
			)
		)
		{
			return;
		}
		while (
			this.waitingTransactions.size() > 0 && this.runningTransactions.size() < this.numRunningPace
		)
		{
			ObjectId transactionId = this.getNextTransactionIdToPass();
			if (transactionId == null)
			{
				break;
			}
			Transaction transaction = this.waitingTransactions.remove(
					transactionId
			);
                        if (!this.runningTransactions.containsKey(transactionId))
                        {
                                this.runningTransactions.put(
                                        transactionId,
                                        transaction
                                );
                                this.schedule(
                                        transactionId
                                );
                        }
		}
	}

	void onTransactionDone(
			ObjectId transactionId
	)
	{
		logger.info(
				this.name + "PaceMaker::onTransactionDone: " + transactionId
		);
		assert this.runningTransactions.containsKey(
				transactionId
		);
		this.runningTransactions.remove(
				transactionId
		);
		++this.numFinished;
		this.scheduleUpToTheLimit();
	}

	@Override
	public Integer getNumFinished()
	{
		assert this.finished.equals(
				new Boolean(
						true
				)
		);
		return this.numFinished;
	}

	Boolean finish;
	Boolean finished;
	TaskRunner taskRunner;
	Thread taskRunnerThread;
	Integer numFinished;
	final String name;
	ObjectId lastCandidate;
	TreeMap<ObjectId, Transaction> waitingTransactions;
	Map<ObjectId, Transaction> runningTransactions;
	final Integer numRunningPace;
	final Integer singleComponentLimit;
	TransactionProcessor transactionProcessor;
	TaskRunner transactionTaskRunner;
	static final Logger logger = LogManager.getLogger();

}
