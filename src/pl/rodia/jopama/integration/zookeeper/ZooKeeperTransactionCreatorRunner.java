package pl.rodia.jopama.integration.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class ZooKeeperTransactionCreatorRunner
{

	public ZooKeeperTransactionCreatorRunner(
			String addresses,
			Integer clusterSize,
			String startFinishDir,
			Integer desiredOutstandingTransactionsNum,
			Long firstComponentId,
			Long numComponents,
			Long numComponentsInTransaction
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
		this.clusterSize = clusterSize;
		this.startFinishDir = startFinishDir;
		this.desiredOutstandingTransactionsNum = desiredOutstandingTransactionsNum;
		this.firstComponentId = firstComponentId;
		this.numComponents = numComponents;
		this.numComponentsInTransaction = numComponentsInTransaction;
		this.init();
	}

	void init()
	{
		this.taskRunnerThread.start();
		this.startFinishDetector = new ZooKeeperDirChangesDetector(
				this.addresses,
				this.clusterSize,
				new Integer(
						0
				),
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
				this.clusterSize,
				this.desiredOutstandingTransactionsNum,
				this.firstComponentId,
				this.numComponents,
				this.numComponentsInTransaction
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
	Integer clusterSize;
	String startFinishDir;
	Integer desiredOutstandingTransactionsNum;
	Long firstComponentId;
	Long numComponents;
	Long numComponentsInTransaction;
	ZooKeeperActorBase startFinishDetector;
	ZooKeeperTransactionCreator transactionCreator;
	static final Logger logger = LogManager.getLogger();

	public static void main(
			String[] args
	)
	{
		assert (args.length == 7);
		String addresses = args[0];
		Integer clusterSize = new Integer(
				Integer.parseInt(
						args[1]
				)
		);
		String startFinishDir = args[2];
		Integer desiredOutstandingTransactionsNum = Integer.parseInt(
				args[3]
		);
		Long firstComponentId = Long.parseLong(
				args[4]
		);
		Long numComponents = Long.parseLong(
				args[5]
		);
		Long numComponentsInTransaction = Long.parseLong(
				args[6]
		);
		ZooKeeperTransactionCreatorRunner transactionCreatorRunner = new ZooKeeperTransactionCreatorRunner(
				addresses,
				clusterSize,
				startFinishDir,
				desiredOutstandingTransactionsNum,
				firstComponentId,
				numComponents,
				numComponentsInTransaction
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
