package pl.rodia.jopama.integration.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class ZooKeeperTransactionCreatorRunner
{

	public ZooKeeperTransactionCreatorRunner(
			String startFinishConnectionString, String startFinishDir, String transactionConnectionString, String transactionDir,
			Integer desiredOutstandingTransactionsNum
	)
	{
		super();
		this.finished = new Boolean(
				false
		);
		this.taskRunner = new TaskRunner(
				"ZooKeeperTransactionCreatorRunner"
		);
		this.taskRunnerThread = new Thread(
				this.taskRunner
		);
		this.startFinishConnectionString = startFinishConnectionString;
		this.startFinishDir = startFinishDir;
		this.transactionConnectionString = transactionConnectionString;
		this.transactionDir = transactionDir;
		this.desiredOutstandingTransactionsNum = desiredOutstandingTransactionsNum;
		this.init();
	}

	void init()
	{
		this.taskRunnerThread.start();
		this.startFinishDetector = new ZooKeeperDirChangesDetector(
				this.startFinishConnectionString,
				this.startFinishDir,
				new StartFinishDetector(
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
												logger.info(
														"ZooKeeperTransactionCreatorRunner::Observer::Start detected"
												);
												start();
											}
										}
								);
							}
						},
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
												logger.info(
														"ZooKeeperTransactionCreatorRunner::Observer::Finish detected"
												);
												finish();

											}
										}
								);
							}
						}
				)
		);
		this.startFinishDetector.start();
	}

	void start()
	{
		logger.info(
				"ZooKeeperTransactionCreatorRunner::start"
		);
		this.transactionCreator = new ZooKeeperTransactionCreator(
				this.transactionConnectionString,
				this.transactionDir,
				this.desiredOutstandingTransactionsNum,
				this.numCreators
		);
		this.transactionCreator.start();
	}

	void finish()
	{
		try
		{
			this.transactionCreator.finish();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		try
		{
			this.startFinishDetector.finish();
		}
		catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		synchronized (this)
		{
			this.finished = new Boolean(
					true
			);
		}
	}

	void waitForFinish() throws InterruptedException
	{
		synchronized (this)
		{
			while (
				this.finished.equals(
						false
				)
			)
			{
				this.wait(1000);
			}
		}
		this.taskRunner.finish();
		try
		{
			this.taskRunnerThread.join();
		}
		catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	Boolean finished;
	TaskRunner taskRunner;
	Thread taskRunnerThread;
	String startFinishConnectionString;
	String startFinishDir;
	String transactionConnectionString;
	String transactionDir;
	Integer desiredOutstandingTransactionsNum;
	Integer numCreators;
	ZooKeeperActorBase startFinishDetector;
	ZooKeeperTransactionCreator transactionCreator;
	static final Logger logger = LogManager.getLogger();

	public static void main(
			String[] args
	)
	{
		assert (args.length == 5);
		String startFinishConnectionString = args[0];
		String startFinishDir = args[1];
		String transactionConnectionString = args[2];
		String transactionDir = args[3];
		Integer desiredOutstandingTransactionsNum = Integer.parseInt(
				args[4]
		);
		ZooKeeperTransactionCreatorRunner transactionCreatorRunner = new ZooKeeperTransactionCreatorRunner(
				startFinishConnectionString,
				startFinishDir,
				transactionConnectionString,
				transactionDir,
				desiredOutstandingTransactionsNum
		);
		try
		{
			transactionCreatorRunner.waitForFinish();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}
}
