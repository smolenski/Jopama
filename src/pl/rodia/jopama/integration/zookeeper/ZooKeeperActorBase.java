package pl.rodia.jopama.integration.zookeeper;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public abstract class ZooKeeperActorBase
{

	public ZooKeeperActorBase(
			String id,
			String addresses,
			Integer clusterSize
	)
	{
		super();
		this.finish = new Boolean(
				false
		);
		this.scheduledTaskId = null;
		this.id = id;
		this.zooKeeperMultiProvider = new ZooKeeperMultiProvider(
				addresses,
				clusterSize
		);
		this.taskRunner = new TaskRunner(
				"ZooKeeperActorBase"
		);
		this.taskRunnerThread = new Thread(
				this.taskRunner
		);
	}

	public Integer schedule(
			Task task
	)
	{
		synchronized (this)
		{

			if (
				this.finish.equals(
						new Boolean(
								false
						)
				)
			)
			{
				return this.taskRunner.schedule(
						task
				);
			}
			else
			{
				return null;
			}
		}
	}

	public Integer schedule(
			Task task, Long delay
	)
	{
		synchronized (this)
		{
			if (
				this.finish.equals(
						new Boolean(
								false
						)
				)
			)
			{
				return this.taskRunner.schedule(
						task,
						delay
				);
			}
			else
			{
				return null;
			}
		}
	}

	public void start()
	{
		this.zooKeeperMultiProvider.start();
		this.taskRunnerThread.start();
		this.scheduledTaskId = this.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						tryToPerformWrapped();
					}
				}
		);
	}

	public void finish() throws InterruptedException, ExecutionException
	{
		logger.info(this.id + " finish");
		synchronized (this)
		{
			this.finish = new Boolean(
					true
			);
		}
		logger.info(this.id + " waiting for task runner");
		this.taskRunner.finish();
		logger.info(this.id + " waiting for thread");
		this.taskRunnerThread.join();
		logger.info(this.id + " waiting for zoo keeper multi provider");
		this.zooKeeperMultiProvider.finish();
	}

	public void scheduleNextIfNotScheduled(
			Long delay
	)
	{
		if (
			this.scheduledTaskId == null
		)
		{
			this.scheduledTaskId = this.schedule(
					new Task()
					{
						@Override
						public void execute()
						{
							tryToPerformWrapped();
						}
					},
					delay
			);
		}
	}

	public void scheduleNextAsap()
	{
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
				).equals(
						new Boolean(
								true
						)
				)
			)
			{
				noTaskScheduled = new Boolean(
						true
				);
			}
		}
		if (
			noTaskScheduled
		)
		{
			this.scheduledTaskId = this.schedule(
					new Task()
					{
						@Override
						public void execute()
						{
							tryToPerformWrapped();
						}
					}
			);
		}
	}

	abstract public Long getRetryDelay();

	abstract public void tryToPerform();

	public void tryToPerformWrapped()
	{
		logger.info(
			this.id + " tryToPerformWrapped start"
		);
		this.scheduledTaskId = null;
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
				logger.info(
					this.id + " tryToPerformWrapped not calling tryToPerform, because finish"
				);
				return;
			}
		}
		this.scheduleNextIfNotScheduled(
				this.getRetryDelay()
		);

		this.tryToPerform();

	}

	Boolean finish;
	Integer scheduledTaskId;
	String id;
	String dir;
	DirChangesObserver dirChangesObserver;
	ZooKeeperMultiProvider zooKeeperMultiProvider;
	TaskRunner taskRunner;
	Thread taskRunnerThread;

	static final Logger logger = LogManager.getLogger();

}
