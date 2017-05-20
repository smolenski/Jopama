package pl.rodia.jopama.integration.zookeeper;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public abstract class ZooKeeperSyncedRunner
{
	public ZooKeeperSyncedRunner(
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
		this.addresses = addresses;
		this.clusterSize = clusterSize;
		this.startFinishDir = startFinishDir;
	}

	void start()
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
	}

	void finish()
	{
		try
		{
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

	abstract void startDetected();

	abstract void finishDetected();

	Boolean finished;
	TaskRunner taskRunner;
	Thread taskRunnerThread;
	String addresses;
	Integer clusterSize;
	String startFinishDir;
	ZooKeeperActorBase startFinishDetector;
	static final Logger logger = LogManager.getLogger();
}
