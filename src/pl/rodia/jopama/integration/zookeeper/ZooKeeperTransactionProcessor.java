package pl.rodia.jopama.integration.zookeeper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.integration.Integrator;
import pl.rodia.mpf.Task;

public class ZooKeeperTransactionProcessor extends ZooKeeperActorBase
{

	public ZooKeeperTransactionProcessor(
			String addresses, Integer clusterSize, Integer clusterId
	)
	{
		super(
				addresses,
				clusterSize
		);
		this.clusterId = clusterId;
	}

	@Override
	public Long getRetryDelay()
	{
		return new Long(
				3000
		);
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

			zooKeeperProvider.zooKeeper.getChildren(
					ZooKeeperHelpers.getTransactionBasePath(),
					null,
					new Children2Callback()
					{
						@Override
						public void processResult(
								int rc, String path, Object ctx, List<String> children, Stat stat
						)
						{
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
												Set<ObjectId> transactionIds = new HashSet<ObjectId>();
												for (String fileName : children)
												{
													String transactionPrefix = "Transaction_";
													assert (
														fileName.startsWith(
																transactionPrefix
														)
													);
													transactionIds.add(new ZooKeeperObjectId(fileName));
												}
												integrator.paceMaker.addTransactions(transactionIds);
											}
										}
								);
							}
						}
					},
					null
			);
		}
	}

	Integer clusterId;
	Integrator integrator;
}
