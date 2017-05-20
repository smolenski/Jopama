package pl.rodia.jopama.integration.zookeeper;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
			String addresses, Integer clusterSize, Integer clusterId, Integer numOutstanding
	)
	{
		super(
				addresses,
				clusterSize
		);
		this.clusterId = clusterId;
		this.integratorZooKeeperMultiProvider = new ZooKeeperMultiProvider(
				addresses,
				clusterSize
		);
		this.integratorZooKeeperStorageGateway = new ZooKeeperStorageGateway(
				zooKeeperMultiProvider
		);
		this.integrator = new Integrator("Integrator", this.integratorZooKeeperStorageGateway, new LinkedList<ObjectId>(), numOutstanding);
	}
	
	@Override
	public void start()
	{
		super.start();
		this.integratorZooKeeperMultiProvider.start();
		this.integrator.start();
	}

	@Override
	public void finish() throws InterruptedException, ExecutionException
	{
		this.integrator.prepareToFinish();
		this.integrator.finish();
		this.integratorZooKeeperMultiProvider.finish();
		super.finish();
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
	ZooKeeperMultiProvider integratorZooKeeperMultiProvider;
	ZooKeeperStorageGateway integratorZooKeeperStorageGateway;
	Integrator integrator;
}
