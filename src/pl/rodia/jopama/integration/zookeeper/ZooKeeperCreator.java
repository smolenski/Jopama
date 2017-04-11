package pl.rodia.jopama.integration.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;

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

	private ZooKeeperObjectId tryToCreateObject(
			ZooKeeperObjectId objectId, byte[] data
	)
	{
		ZooKeeperProvider zooKeeperProvider = this.zooKeeperMultiProvider.getResponsibleProvider(
				objectId.getClusterId(
						this.zooKeeperMultiProvider.getNumClusters()
				)
		);
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
					logger.debug(
							"zooKeeper.create calling"
					);
					String result = zooKeeperProvider.zooKeeper.create(
							ZooKeeperHelpers.getPath(
									objectId
							),
							data,
							Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT
					);
					logger.debug(
							"zooKeeper.create finished success (path: " + result + ")"
					);
					return ZooKeeperHelpers.getIdFromPath(
							result
					);
				}
				catch (KeeperException | InterruptedException e)
				{
					logger.debug(
							"zooKeeper.create finished failure"
					);
					logger.error(
							"zooKeeper.create failed: " + e
					);
					return null;
				}
			}
		}
	}

	public ZooKeeperObjectId createObject(
			ZooKeeperObjectId objectId, byte[] data
	)
	{
		for (int i = 0; i < ZooKeeperCreator.NUM_TRIES.intValue(); ++i)
		{
			logger.debug(
					"zooKeeper.create (try: " + i + ")"
			);
			ZooKeeperObjectId path = this.tryToCreateObject(
					objectId,
					data
			);
			if (
				path != null
			)
			{
				return path;
			}
			try
			{
				Thread.sleep(
						1000
				);
			}
			catch (InterruptedException e)
			{
				logger.debug(
						"Sleeping"
				);
			}
		}
		return null;
	}

	static Integer NUM_TRIES = 3;
	ZooKeeperMultiProvider zooKeeperMultiProvider;
	static final Logger logger = LogManager.getLogger();
}
