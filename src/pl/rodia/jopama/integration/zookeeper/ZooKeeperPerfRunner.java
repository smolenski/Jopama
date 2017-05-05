package pl.rodia.jopama.integration.zookeeper;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class ZooKeeperPerfRunner
{

	public ZooKeeperPerfRunner(
			String connectionString, String startFinishDir
	)
	{
		super();
		this.scheduledTaskId = null;
		this.startAppearanceReported = new Boolean(
				false
		);
		this.finishAppearanceReported = new Boolean(
				false
		);
		this.startFinishDir = startFinishDir;
		this.zooKeeperProvider = new ZooKeeperProvider(
				"ZooKeeperPerfRunner.zooKeeperConnectionProvider",
				connectionString
		);
		this.taskRunner = new TaskRunner(
				"ZooKeeperPerfRunner"
		);
		this.taskRunnerThread = new Thread(
				this.taskRunner
		);
	}

	public void start()
	{
		this.zooKeeperProvider.start();
		this.taskRunnerThread.start();
		this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						getListing();
					}
				}
		);
	}

	public void finish() throws InterruptedException
	{
		this.taskRunner.finish();
		this.taskRunnerThread.join();
		this.zooKeeperProvider.finish();
	}

	public void scheduleGetListing()
	{
		this.scheduledTaskId = this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						getListing();
					}
				},
				new Long(
						3000
				)
		);
	}

	public void scheduleGetListingAsap()
	{
		Boolean noTaskScheduled = new Boolean(false);
		if (this.scheduledTaskId == null)
		{
			noTaskScheduled = new Boolean(true);
		}
		else
		{
			if (this.taskRunner.cancelTask(this.scheduledTaskId))
			{
				noTaskScheduled = new Boolean(true);
			}
		}
		if (noTaskScheduled)
		{
			this.scheduledTaskId = this.taskRunner.schedule(
					new Task()
					{
						@Override
						public void execute()
						{
							getListing();
						}
					}
			);
		}
	}

	public void getListing()
	{
		this.scheduledTaskId = null;
		if (finishAppearanceReported.equals(new Boolean(true)))
		{
			return;
		}
		this.scheduleGetListing();
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
				this.zooKeeperProvider.zooKeeper.getChildren(
						this.startFinishDir,
						new Watcher()
						{
							@Override
							public void process(
									WatchedEvent event
							)
							{
								taskRunner.schedule(
										new Task()
										{
											@Override
											public void execute()
											{
												scheduleGetListingAsap();
											}
										}
								);
							}
						},
						new Children2Callback()
						{
							@Override
							public void processResult(
									int rc, String path, Object ctx, List<String> children, Stat stat
							)
							{
								taskRunner.schedule(
										new Task()
										{
											@Override
											public void execute()
											{
												processListing(
														children
												);
											}
										}
								);
							}
						},
						null
				);
			}
		}
	}

	public void processListing(
			List<String> children
	)
	{
		StringBuilder childrenNames = new StringBuilder();
		childrenNames.append("[");
		for (String child : children)
		{
			childrenNames.append(child + ",");
		}
		childrenNames.append("]");
		logger.info("Children: " + childrenNames.toString());
		if (
			startAppearanceReported.equals(
					new Boolean(
							false
					)
			)
		)
		{
			if (
				children.contains(
						START_NAME
				)
			)
			{
				reportStart();
				this.startAppearanceReported = new Boolean(true);
			}
		}
		if (
			finishAppearanceReported.equals(
					new Boolean(
							false
					)
			)
		)
		{
			if (
				children.contains(
						FINISH_NAME
				)
			)
			{
				reportFinish();
				this.finishAppearanceReported = new Boolean(true);
				if (this.scheduledTaskId != null)
				{
					this.taskRunner.cancelTask(this.scheduledTaskId);
				}
			}
		}
	}

	public void reportStart()
	{
		logger.info("START OBSERVED");
	}

	public void reportFinish()
	{
		logger.info("FINISH OBSERVED");
	}

	Integer scheduledTaskId;
	Boolean startAppearanceReported;
	Boolean finishAppearanceReported;
	String startFinishDir;
	ZooKeeperProvider zooKeeperProvider;
	TaskRunner taskRunner;
	Thread taskRunnerThread;
	static final Logger logger = LogManager.getLogger();
	static final String START_NAME = "START";
	static final String FINISH_NAME = "FINISH";
	
	
	public static void main(
			String[] args
	)
	{
		assert (args.length == 2);
		String connectionString = args[0];
		String startFinishDir = args[1];
		ZooKeeperPerfRunner perfRunner = new ZooKeeperPerfRunner(connectionString, startFinishDir);
		perfRunner.start();
		try
		{
			perfRunner.finish();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}

}
