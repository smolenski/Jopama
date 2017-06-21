package pl.rodia.jopama.integration.zookeeper;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public abstract class ZooKeeperSyncedRunner
{
	public ZooKeeperSyncedRunner(
			String id,
			String addresses,
			Integer clusterSize,
			String startFinishDir
	)
	{
		super();
		this.finished = new Boolean(
				false
		);
		this.taskRunner = new TaskRunner(
				"ZooKeeperSyncedRunner"
		);
		this.taskRunnerThread = new Thread(
				this.taskRunner
		);
		this.id = id;
		this.addresses = addresses;
		this.clusterSize = clusterSize;
		this.startFinishDir = startFinishDir;
	}

	void start()
	{
		logger.info(this.id + " starting");
		this.taskRunnerThread.start();
		this.startFinishDetector = new ZooKeeperDirChangesDetector(
				this.id + "::StartFinishDetector",
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
														"ZooKeeperSyncedRunner::Observer::Start detected"
												);
												startDetected();
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
														"ZooKeeperSyncedRunner::Observer::Finish detected"
												);
												finishDetected();

											}
										}
								);
							}
						}
				)
		);
		this.startFinishDetector.start();
		this.createReadyNode();
		logger.info(this.id + " start done");
	}

	void finish()
	{
		logger.info(this.id + " finishing");
		try
		{
			logger.info(this.id + " finishing start finish detector");
			this.startFinishDetector.finish();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		catch (ExecutionException e)
		{
			e.printStackTrace();
		}
		logger.info(this.id + " setting finish flag");
		synchronized (this)
		{
			this.finished = new Boolean(
					true
			);
		}
		logger.info(this.id + " finish done");
	}

	void waitForFinish() throws InterruptedException
	{
		logger.info(this.id + " waiting for finish");
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
		logger.info(this.id + " finishing task runner");
		this.taskRunner.finish();
		try
		{
			logger.info(this.id + " waiting for task runner thread finish");
			this.taskRunnerThread.join();
			this.createDoneNode();
		}
		catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info(this.id + " waiting for finish done");
	}

	abstract void startDetected();

	abstract void finishDetected();
	
	abstract String getReadyPath();
	
	abstract String getDonePath();
	
	abstract String getReadyString();
	
	abstract String getDoneString();
	
	void createReadyNode()
	{
		this.createNode(
			this.getReadyPath(),
			this.getReadyString()
		);
	}
	
	void createDoneNode()
	{
		this.createNode(
			this.getDonePath(),
			this.getDoneString()
		);
	}
	
	void createNode(String path, String content)
	{
		logger.info(this.id + " creating node, path: " + path + " content: " + content);
		ZooKeeperFileCreator fileCreator = new ZooKeeperFileCreator(
			this.id + "::FileCreator",
			this.addresses,
			this.clusterSize,
			new Integer(0),
			path,
			content
		);
		fileCreator.start();
		try
		{
			fileCreator.waitUntilCreated();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		try
		{
			fileCreator.finish();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		catch (ExecutionException e)
		{
			e.printStackTrace();
		}
		logger.info(this.id + " creating node done");
	}

	Boolean finished;
	TaskRunner taskRunner;
	Thread taskRunnerThread;
	String id;
	String addresses;
	Integer clusterSize;
	String startFinishDir;
	ZooKeeperActorBase startFinishDetector;
	static final Logger logger = LogManager.getLogger();
}
