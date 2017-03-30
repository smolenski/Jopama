package pl.rodia.jopama.integration.zookeeper;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;

public class ZooKeeperCreator
{

	public ZooKeeperCreator(
			String name, String connectionString
	)
	{
		this.zooKeeperProvider = new ZooKeeperProvider(
				name,
				connectionString
		);
	}

	public void start()
	{
		this.zooKeeperProvider.start();
	}

	public void stop() throws InterruptedException
	{
		this.zooKeeperProvider.finish();
	}

	private Boolean tryToCreateObject(
			String path, byte[] data
	)
	{
		synchronized (this.zooKeeperProvider)
		{
			if (
				this.zooKeeperProvider.zooKeeper == null
						||
						this.zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
			)
			{
				return new Boolean(
						false
				);
			}
			else
			{
				try
				{
					logger.debug("zooKeeper.create calling");
					this.zooKeeperProvider.zooKeeper.create(
							path,
							data,
							Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT
					);
					logger.debug("zooKeeper.create finished success");
					return new Boolean(
							true
					);
				}
				catch (KeeperException | InterruptedException e)
				{
					logger.debug("zooKeeper.create finished failure");
					logger.error(
							"zooKeeper.create failed: " + e
					);
					return new Boolean(
							false
					);
				}
			}
		}
	}
	
	public Boolean createObject(String path, byte [] data)
	{
		for (int i = 0; i < ZooKeeperCreator.NUM_TRIES.intValue(); ++i)
		{
			logger.debug("zooKeeper.create (try: " + i + ")");
			if (this.tryToCreateObject(path, data).equals(new Boolean(true)))
			{
				return new Boolean(true);
			}
			try
			{
				Thread.sleep(1000);
			}
			catch (InterruptedException e)
			{
				logger.debug("Sleeping");
			}
		}
		return new Boolean(false);
	}
	
	static Integer NUM_TRIES = 3;
	ZooKeeperProvider zooKeeperProvider;
	static final Logger logger = LogManager.getLogger();
}
