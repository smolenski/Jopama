package pl.rodia.jopama.integration.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import pl.rodia.jopama.integration.ExtendedData;

public class ZooKeeperStorageAccess
{

	public ZooKeeperStorageAccess(
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
		for (int i = 0; i < ZooKeeperStorageAccess.NUM_TRIES.intValue(); ++i)
		{
			logger.debug(
					"zooKeeper.createObject (try: " + i + ")"
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

	private ExtendedData tryToReadObject(
			ZooKeeperObjectId objectId
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
					String path = ZooKeeperHelpers.getPath(
							objectId
					);
					logger.debug(
							"zooKeeper.readObject calling (path: " + path + ")"
					);
					Stat stat = new Stat();
					byte[] data = zooKeeperProvider.zooKeeper.getData(
							path,
							null,
							stat
					);
					logger.debug(
							"zooKeeper.create finished success (path: " + path + ")"
					);
					return new ExtendedData(
							data,
							new Integer(
									stat.getVersion()
							)
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

	public ExtendedData readObject(ZooKeeperObjectId objectId)
	{
		for (int i = 0; i < ZooKeeperStorageAccess.NUM_TRIES.intValue(); ++i)
		{
			logger.debug(
					"zooKeeper.readObject (try: " + i + ")"
			);
			ExtendedData data = this.tryToReadObject(
					objectId
			);
			if (
				data != null
			)
			{
				return data;
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
