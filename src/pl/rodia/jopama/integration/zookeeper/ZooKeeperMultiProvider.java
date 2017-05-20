package pl.rodia.jopama.integration.zookeeper;

import java.util.LinkedList;
import java.util.List;

public class ZooKeeperMultiProvider
{

	public ZooKeeperMultiProvider(
			String addresses, Integer zooKeeperClusterSize
	)
	{
		final List<String> connectionStrings = ZooKeeperHelpers.prepareZooKeeperClustersConnectionStrings(
				addresses,
				zooKeeperClusterSize
		);
		this.zooKeeperProvideers = new LinkedList<ZooKeeperProvider>();
		for (int i = 0; i < connectionStrings.size(); ++i)
		{
			this.zooKeeperProvideers.add(
					new ZooKeeperProvider(
							"ZooKeeperProvider_" + i,
							connectionStrings.get(
									i
							)
					)
			);
		}
	}

	public void start()
	{
		for (ZooKeeperProvider zooKeeperProvider : this.zooKeeperProvideers)
		{
			zooKeeperProvider.start();
		}
	}

	public void finish() throws InterruptedException
	{
		for (ZooKeeperProvider zooKeeperProvider : this.zooKeeperProvideers)
		{
			zooKeeperProvider.finish();
		}
	}

	ZooKeeperProvider getResponsibleProvider(
			Integer id
	)
	{
		return this.zooKeeperProvideers.get(
				Math.floorMod(id, this.zooKeeperProvideers.size())
		);
	}
	
	Integer getNumClusters()
	{
		return this.zooKeeperProvideers.size();
	}

	final List<ZooKeeperProvider> zooKeeperProvideers;
}
