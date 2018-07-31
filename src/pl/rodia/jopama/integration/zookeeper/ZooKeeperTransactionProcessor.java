package pl.rodia.jopama.integration.zookeeper;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.integration.Integrator;
import pl.rodia.jopama.stats.StatsAsyncSource;
import pl.rodia.jopama.stats.StatsCollector;
import pl.rodia.mpf.Task;

public class ZooKeeperTransactionProcessor extends ZooKeeperActorBase
{

	public ZooKeeperTransactionProcessor(
		String id,
		String addresses,
		Integer clusterSize,
		Integer clusterId,
		Integer numOutstanding
	)
	{
		super(
				id,
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
		
		this.statsCollector = new StatsCollector(
				this.integrator.getStatsSources()
		);
	}
	
	@Override
	public void start()
	{
		logger.info("transaction processor start");
		super.start();
		this.integratorZooKeeperMultiProvider.start();
		this.integrator.start();
		this.statsCollector.start();
		logger.info("transaction processor start done");
	}

	@Override
	public void finish() throws InterruptedException, ExecutionException
	{
		logger.info("transaction processor finish");
		this.statsCollector.prepareToFinish();
		this.integrator.prepareToFinish();
		this.statsCollector.finish();
		this.integrator.finish();
		this.integratorZooKeeperMultiProvider.finish();
		super.finish();
		logger.info("transaction processor finish done");
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
	StatsCollector statsCollector;
	Integrator integrator;
	static final Logger logger = LogManager.getLogger();
}
