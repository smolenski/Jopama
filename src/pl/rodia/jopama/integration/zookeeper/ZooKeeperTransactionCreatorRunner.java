package pl.rodia.jopama.integration.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class ZooKeeperTransactionCreatorRunner
{

	public ZooKeeperTransactionCreatorRunner(
			String addresses,
			Integer numClusters,
			String startFinishDir,
			String transactionDir,
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
		this.addresses = addresses;
		this.numClusters = numClusters;
		this.startFinishDir = startFinishDir;
		this.transactionDir = transactionDir;
		this.desiredOutstandingTransactionsNum = desiredOutstandingTransactionsNum;
		this.init();
	}

	void init()
	{
		this.taskRunnerThread.start();
		this.startFinishDetector = new ZooKeeperDirChangesDetector(
				this.addresses,
				this.numClusters,
				new Integer(0),
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
				this.addresses,
				this.numClusters,
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
				this.wait(
						1000
				);
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
	String addresses;
	Integer numClusters;
	String startFinishDir;
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
		String addresses = args[0];
		Integer numClusters = new Integer(Integer.parseInt(args[1]));
		String startFinishDir = args[2];
		String transactionDir = args[3];
		Integer desiredOutstandingTransactionsNum = Integer.parseInt(
				args[4]
		);
		ZooKeeperTransactionCreatorRunner transactionCreatorRunner = new ZooKeeperTransactionCreatorRunner(
				addresses,
				numClusters,
				startFinishDir,
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
