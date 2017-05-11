package pl.rodia.jopama.integration.zookeeper;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import pl.rodia.mpf.Task;

public class ZooKeeperDirChangesDetector extends ZooKeeperActorBase
{

	public ZooKeeperDirChangesDetector(
			String addresses, Integer clusterSize, Integer clusterId, String dir, DirChangesObserver dirChangesObserver
	)
	{
		super(
				addresses,
				clusterSize
		);
		this.clusterId = clusterId;
		this.dir = dir;
		this.dirChangesObserver = dirChangesObserver;
	}

	@Override
	public Long getRetryDelay()
	{
		return new Long(
				3000
		);
	}

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

			zooKeeperProvider.zooKeeper.getChildren(
					this.dir,
					new Watcher()
					{
						@Override
						public void process(
								WatchedEvent event
						)
						{
							schedule(
									new Task()
									{
										@Override
										public void execute()
										{
											scheduleNextAsap();
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
							logger.info(
									"getListing done, dir: " + dir
							);
							if (
								rc == KeeperException.Code.OK.intValue()
							)
							{
								schedule(
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
							else
							{
								logger.error(
										"getListing failed, rc: " + rc
								);
							}
						}
					},
					null
			);
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

	Integer clusterId;
	String dir;
	DirChangesObserver dirChangesObserver;

	static final Logger logger = LogManager.getLogger();

}
