package pl.rodia.jopama.integration.zookeeper;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class ZooKeeperTransactionCreator
{

	public ZooKeeperTransactionCreator(
			String connectionString, String transactionDir, Integer desiredOutstandingTransactionsNum, Integer numCreators
	)
	{
		this.finish = new Boolean(
				false
		);
		this.scheduledTaskId = null;
		this.numFiles = null;
		this.connectionString = connectionString;
		this.transactionDir = transactionDir;
		this.desiredOutstandingTransactionsNum = desiredOutstandingTransactionsNum;
		this.numCreators = numCreators;
		this.taskRunner = new TaskRunner(
				"ZooKeeperTransactionCreator"
		);
		this.taskRunnerThread = new Thread(
				this.taskRunner
		);
		this.zooKeeperProvider = new ZooKeeperProvider(
				"ZooKeeperTransactionCreator.ZooKeeperProvider",
				connectionString
		);
		this.dirChangesDetector = new ZooKeeperDirChangesDetector(
				connectionString,
				transactionDir,
				new DirChangesObserver()
				{
					@Override
					public void directoryContentChanged(
							List<String> fileNames
					)
					{
						taskRunner.schedule(
								new Task()
								{
									@Override
									public void execute()
									{
										numFiles = new Integer(
												fileNames.size()
										);
										Integer numFilesToCreate = (desiredOutstandingTransactionsNum - numFiles) / numCreators;
										if (numFilesToCreate.compareTo(new Integer(0)) > 0)
										{
											scheduleCreateFilesUpToTheTresholdAsap();
										}
									}
								}
						);
					}
				}
		);
	}

	void scheduleCreateFilesUpToTheTresholdAsap()
	{
		logger.info("TransactionCreator::scheduleCreateFilesUpToTheTresholdAsap running");
		Boolean noTaskScheduled = new Boolean(
				false
		);
		if (
			this.scheduledTaskId == null
		)
		{
			noTaskScheduled = new Boolean(
					true
			);
		}
		else
		{
			if (
				this.taskRunner.cancelTask(
						this.scheduledTaskId
				).equals(new Boolean(true))
			)
			{
				noTaskScheduled = new Boolean(
						true
				);
			}
		}
		if (
			noTaskScheduled.equals(new Boolean(true))
		)
		{
			logger.info("TransactionCreator::scheduleCreateFilesUpToTheTresholdAsap new task");
			this.scheduledTaskId = this.taskRunner.schedule(
					new Task()
					{
						@Override
						public void execute()
						{
							createFilesUpToTheTreshold();
						}
					}
			);
		}
	}

	void scheduleCreateFilesUpToTheTreshold()
	{
		this.scheduledTaskId = this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						createFilesUpToTheTreshold();
					}
				},
				new Long(
						3000
				)
		);
	}

	void createFilesUpToTheTreshold()
	{
		this.scheduledTaskId = null;
		logger.info("TransactionCreator::createFilesUpToTheTreshold running");
		synchronized (this)
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
		}
		assert this.numFiles != null;
		assert this.desiredOutstandingTransactionsNum.compareTo(
				this.numFiles
		) >= 0;
		Integer numFilesToCreate = (this.desiredOutstandingTransactionsNum - this.numFiles) / this.numCreators;
		logger.info("ZooKeeperTransactionCreator::createFilesUpToTheTreshold, numFilesToCreate: " + numFilesToCreate);
		synchronized (this.zooKeeperProvider)
		{
			if (
				this.zooKeeperProvider.zooKeeper == null
						||
						this.zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
			)
			{
				this.scheduleCreateFilesUpToTheTreshold();
			}
			else
			{
				for (int i = 0; i < numFilesToCreate.intValue(); ++i)
				{
					byte[] serializedTransaction = new byte[10];
					this.zooKeeperProvider.zooKeeper.create(
							this.transactionDir + "/",
							serializedTransaction,
							Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT_SEQUENTIAL,
							new StringCallback()
							{
								@Override
								public void processResult(
										int rc, String path, Object ctx, String name
								)
								{
									logger.info("ZooKeeperTransactionCreator fileCreated, i: " + ctx);
								}
							},
							new Integer(i)
					);
				}
			}
		}

	}

	void start()
	{
		this.taskRunnerThread.start();
		this.zooKeeperProvider.start();
		this.dirChangesDetector.start();
	}

	void finish() throws InterruptedException
	{
		this.dirChangesDetector.finish();
		this.zooKeeperProvider.finish();
		synchronized (this)
		{
			this.finish = new Boolean(
					true
			);
		}
		this.taskRunner.finish();
		this.taskRunnerThread.join();
	}

	Boolean finish;
	Integer scheduledTaskId;
	Integer numFiles;
	String connectionString;
	String transactionDir;
	Integer desiredOutstandingTransactionsNum;
	Integer numCreators;
	TaskRunner taskRunner;
	Thread taskRunnerThread;
	ZooKeeperProvider zooKeeperProvider;
	ZooKeeperDirChangesDetector dirChangesDetector;
	static final Logger logger = LogManager.getLogger();
}
