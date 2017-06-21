package pl.rodia.jopama.integration.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;

public class ZooKeeperFileCreator extends ZooKeeperActorBase
{
	public ZooKeeperFileCreator(
			String id,
			String addresses,
			Integer clusterSize,
			Integer clusterId,
			String path,
			String content
	)
	{
		super(
				id,
				addresses,
				clusterSize
		);
		this.created = new Boolean(false);
		this.clusterId = clusterId;
		this.path = path;
		this.content = content;
	}

	@Override
	public Long getRetryDelay()
	{
		return new Long(2000);
	}

	@Override
	public void tryToPerform()
	{
		ZooKeeperProvider zooKeeperProvider = zooKeeperMultiProvider.getResponsibleProvider(
				this.clusterId
		);
		synchronized (zooKeeperProvider)
		{

			if (
				zooKeeperProvider.zooKeeper == null
						||
						zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
			)
			{
				return;
			}

			zooKeeperProvider.zooKeeper.create(
				this.path,
				this.content.getBytes(),
				Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT,
				new StringCallback()
				{
					@Override
					public void processResult(
							int rc, String path, Object ctx, String name
					)
					{
						if (rc == KeeperException.Code.OK.intValue())
						{
							onCreated();	
						}
					}
				},
				null
			);
		}

	}
	
	void onCreated()
	{
		synchronized (this)
		{
			this.created = new Boolean(true);
			this.notifyAll();
		}
	}
	
	void waitUntilCreated() throws InterruptedException
	{
		synchronized (this)
		{
			while (this.created.equals(new Boolean(false)))
			{
				this.wait();
			}
		}
	}

	Boolean created;
	Integer clusterId;
	String path;
	String content;

	static final Logger logger = LogManager.getLogger();

	
}
