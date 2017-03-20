package pl.rodia.jopama.integration.zookeeper;
import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.TransactionChange;
import pl.rodia.jopama.gateway.NewComponentVersionFeedback;
import pl.rodia.jopama.gateway.NewTransactionVersionFeedback;
import pl.rodia.jopama.gateway.RemoteStorageGateway;


import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class ZooKeeperProvider implements Watcher
{

	public ZooKeeperProvider(
			String name, String connectionString
	)
	{
		this.name = name;
		this.connectionString = connectionString;
		this.finish = new Boolean(
				true
		);
		this.scheduledTask = null;
		this.taskRunner = new TaskRunner(
				name
		);
		this.taskRunnerThread = new Thread(
				this.taskRunner
		);
		this.zooKeeper = null;
	}

	synchronized void scheduleCreateZooKeeper(
			Long delay
	)
	{
		this.scheduledTask = this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						createZooKeeperIfNeeded();
					}
				},
				new Long(
						delay
				)
		);
	}

	synchronized void createZooKeeperIfNeeded()
	{
		this.scheduledTask = null;
		if (
			this.zooKeeper == null
		)
		{
			try
			{
				this.zooKeeper = new ZooKeeper(
						this.connectionString,
						10 * 1000,
						this
				);
			}
			catch (IOException e)
			{
			}
		}
		if (
			finish.equals(
					new Boolean(
							false
					)
			)
		)
		{
			this.scheduleCreateZooKeeper(
					new Long(
							1000
					)
			);
		}
	}

	synchronized public void start()
	{
		this.taskRunnerThread.start();
		this.scheduleCreateZooKeeper(
				new Long(
						0
				)
		);
	}

	public void finish() throws InterruptedException
	{
		synchronized (this)
		{
			this.finish = new Boolean(
					true
			);
			if (
				this.scheduledTask != null
			)
			{
				this.taskRunner.cancelTask(
						this.scheduledTask
				);
			}
		}
		this.taskRunner.finish();
		this.taskRunnerThread.join();
	}

	@Override
	synchronized public void process(
			WatchedEvent event
	)
	{
		if (
			this.finish.equals(
					new Boolean(
							false
					)
			)
		)
		{
			switch (event.getState())
			{
				case Disconnected:
					break;
				case SyncConnected:
					break;
				case Expired:
					this.zooKeeper = null;
					break;
				default:
					assert false;
					break;
			}
		}
	}
	
	String name;
	String connectionString;
	Boolean finish;
	Integer scheduledTask;
	ZooKeeper zooKeeper;
	TaskRunner taskRunner;
	Thread taskRunnerThread;
}
