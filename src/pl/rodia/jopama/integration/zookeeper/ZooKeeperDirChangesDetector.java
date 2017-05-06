package pl.rodia.jopama.integration.zookeeper;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class ZooKeeperDirChangesDetector
{

	public ZooKeeperDirChangesDetector(
			String connectionString, String dir, DirChangesObserver dirChangesObserver
	)
	{
		super();
		this.finish = new Boolean(
				false
		);
		this.scheduledTaskId = null;
		this.dir = dir;
		this.dirChangesObserver = dirChangesObserver;
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
			noTaskScheduled
		)
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
		logger.info("getListing start, dir: " + dir);
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
				return;
			}
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
						this.dir,
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
								logger.info("getListing done, dir: " + dir);
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
		dirChangesObserver.directoryContentChanged(
				children
		);
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
