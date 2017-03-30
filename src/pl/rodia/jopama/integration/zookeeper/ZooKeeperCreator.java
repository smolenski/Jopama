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
import org.apache.zookeeper.data.Stat;

public class ZooKeeperCreator
{

	public ZooKeeperCreator(
			String addresses,
			Integer clusterSize
	)
	{
		this.zooKeeperMultiProvider = new ZooKeeperMultiProvider(
			addresses,
			clusterSize
		);
	}

	public void start()
	{
		this.zooKeeperMultiProvider.start();
	}

	public void stop() throws InterruptedException
	{
		this.zooKeeperMultiProvider.finish();
	}

	private String tryToCreateObject(
			Integer clusterId, ZooKeeperGroup group, byte[] data
	)
	{
		ZooKeeperProvider zooKeeperProvider = this.zooKeeperMultiProvider.getResponsibleProvider(clusterId);
		synchronized (zooKeeperProvider)
		{
			if (
				zooKeeperProvider.zooKeeper == null
						||
						zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
			)
			{
				return null;
			}
			else
			{
				try
				{
					logger.debug("zooKeeper.create calling");
					String result = zooKeeperProvider.zooKeeper.create(
							ZooKeeperHelpers.getBasePath(group),
							data,
							Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT_SEQUENTIAL
					);
					logger.debug("zooKeeper.create finished success (path: " + result + ")");
					return result;
				}
				catch (KeeperException | InterruptedException e)
				{
					logger.debug("zooKeeper.create finished failure");
					logger.error(
							"zooKeeper.create failed: " + e
					);
					return null;
				}
			}
		}
	}
	
	public String createObject(Integer clusterId, ZooKeeperGroup group, byte [] data)
	{
		for (int i = 0; i < ZooKeeperCreator.NUM_TRIES.intValue(); ++i)
		{
			logger.debug("zooKeeper.create (try: " + i + ")");
			String path = this.tryToCreateObject(clusterId, group, data);
			if (path != null)
			{
				return path;
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
		return null;
	}
	
	static Integer NUM_TRIES = 3;
	ZooKeeperMultiProvider zooKeeperMultiProvider;
	static final Logger logger = LogManager.getLogger();
}
