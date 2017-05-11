package pl.rodia.jopama.integration.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.ZooKeeper.States;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public abstract class ZooKeeperActorBase
{

	public ZooKeeperActorBase(
			String connectionString
	)
	{
		super();
		this.finish = new Boolean(
				false
		);
		this.scheduledTaskId = null;
		this.zooKeeperProvider = new ZooKeeperProvider(
				"ZooKeeperDirChangesDetector.ZooKeeperProvider",
				connectionString
		);
		this.taskRunner = new TaskRunner(
				"ZooKeeperDirChangesDetector"
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
		this.zooKeeperProvider.start();
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

	public void finish() throws InterruptedException
	{
		synchronized (this)
		{
			this.finish = new Boolean(
					true
			);
		}
		this.taskRunner.finish();
		this.taskRunnerThread.join();
		this.zooKeeperProvider.finish();
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
				"tryToPerformWrapped start"
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
						"tryToPerformWrapped not calling tryToPerform, because finish"
				);
				return;
			}
		}
		this.scheduleNextIfNotScheduled(
				this.getRetryDelay()
		);
		synchronized (this.zooKeeperProvider)
		{
			if (
				this.zooKeeperProvider.zooKeeper == null
						||
						this.zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
			)
			{
				return;
			}
			else
			{
				this.tryToPerform();
			}
		}
	}

	Boolean finish;
	Integer scheduledTaskId;
	String dir;
	DirChangesObserver dirChangesObserver;
	ZooKeeperProvider zooKeeperProvider;
	TaskRunner taskRunner;
	Thread taskRunnerThread;

	static final Logger logger = LogManager.getLogger();

}
