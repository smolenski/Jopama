package pl.rodia.jopama.integration.zookeeper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.integration.Integrator;
import pl.rodia.jopama.stats.StatsCollector;
import pl.rodia.mpf.Task;

public class ZooKeeperTransactionProcessor extends ZooKeeperActorBase
{

	public ZooKeeperTransactionProcessor(
		String id,
		String addresses,
		Integer clusterSize,
		Integer clusterId,
		Integer numOutstanding,
		Integer singleComponentLimit
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
		this.singleComponentLimit = singleComponentLimit;
		this.currentTransactions = new HashMap<Long, Transaction>();
		this.numAtomicAsyncOperations = new AtomicInteger(0);
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
				2000
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
            if (this.numAtomicAsyncOperations.get() > 0)
            {
                return;
            }
            this.numAtomicAsyncOperations.set(1);

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
												for (String fileName : children)
												{
													assert (fileName.startsWith(
															ZooKeeperObjectId.transactionPrefix
													));
												}
												tryToPerformCont(
														children
												);
											}
										}
								);
							}
							else
							{
								assert numAtomicAsyncOperations.get() == 1;
	                            numAtomicAsyncOperations.set(0);
							}
						}
					},
					null
			);
		}
	}
	
	public void tryToPerformCont(
			List<String> children
	)
	{
		assert numAtomicAsyncOperations.get() == 1;
		Set<Long> existingIds = new HashSet<Long>();
		for (String fileName : children)
		{
			ZooKeeperObjectId zooKeeperObjectId = new ZooKeeperObjectId(fileName);
			Long tId = zooKeeperObjectId.getId();
			boolean res = existingIds.add(tId);
			assert(res == true);
		}
		Set<Long> idsToRemove = new HashSet<Long>();
		for (Map.Entry<Long, Transaction> entry : this.currentTransactions.entrySet())
		{
			if (!existingIds.contains(entry.getKey()))
			{
				boolean res = idsToRemove.add(entry.getKey());
				assert(res == true);
			}
		}
		for (Long tId : idsToRemove)
		{
			Transaction result = this.currentTransactions.remove(tId);
			assert(result != null);
		}
		Set<Long> toConsider = new HashSet<Long>();
		for (Long tId : existingIds)
		{
			if (!this.currentTransactions.containsKey(tId))
			{
				toConsider.add(tId);
			}
		}
		numAtomicAsyncOperations.set(toConsider.size());
		for (Long tId : toConsider)
		{
			ZooKeeperObjectId objectId = new ZooKeeperObjectId(ZooKeeperObjectId.getTransactionUniqueName(tId));
			ZooKeeperProvider zooKeeperProvider = this.zooKeeperMultiProvider.getResponsibleProvider(
					objectId.getClusterId(this.zooKeeperMultiProvider.getNumClusters())
			);
			synchronized (zooKeeperProvider)
			{
				if (
					zooKeeperProvider.zooKeeper == null
							||
							zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
				)
				{
	                Integer numAsyncBefore = new Integer(this.numAtomicAsyncOperations.getAndDecrement());
	                assert numAsyncBefore.compareTo(new Integer(0)) > 0;
					return;
				}
				else
				{
					zooKeeperProvider.zooKeeper.getData(
						ZooKeeperHelpers.getTransactionPath(objectId),
						false,
						new DataCallback()
						{
							@Override
							public void processResult(
									int rc, String path, Object ctx, byte[] data, Stat stat
							)
							{
								if (rc == KeeperException.Code.OK.intValue())
								{
									schedule(
										new Task()
										{
											@Override
											public void execute()
											{
												Transaction transaction = ZooKeeperHelpers.deserializeTransaction(data);
												SortedMap<Long, Long> compsCount = new TreeMap<Long, Long>();
												for (Map.Entry<Long, Transaction> entry : currentTransactions.entrySet())
												{
													ZooKeeperTransactionCreatorHelpers.updateCompCount(compsCount, entry.getValue());
												}
												Set<Long> compIds = ZooKeeperTransactionCreatorHelpers.getCompIds(transaction);												
												Boolean belowLimit = ZooKeeperTransactionCreatorHelpers.allCompsBelowLimit(singleComponentLimit, compsCount, compIds);
												if (belowLimit.equals(new Boolean(true)))
												{
													Transaction res = currentTransactions.put(objectId.getId(), transaction);
													assert(res == null);
													Set<ObjectId> tIds = new HashSet<ObjectId>();
													tIds.add(objectId);
													integrator.paceMaker.addTransactions(tIds);
												}
												Integer numAsyncBefore = new Integer(numAtomicAsyncOperations.getAndDecrement());
			                                    assert numAsyncBefore.compareTo(new Integer(0)) > 0;
											}
										}
									);
								}
								else
								{
									Integer numAsyncBefore = new Integer(numAtomicAsyncOperations.getAndDecrement());
                                    assert numAsyncBefore.compareTo(new Integer(0)) > 0;
								}
							}
						},
						null
					);
				}
			}
			
		}
	}

	Integer clusterId;
	ZooKeeperMultiProvider integratorZooKeeperMultiProvider;
	ZooKeeperStorageGateway integratorZooKeeperStorageGateway;
	Integer singleComponentLimit;
	Map<Long, Transaction> currentTransactions;
	SortedMap<Long, Long> compsCount;
	AtomicInteger numAtomicAsyncOperations;
	StatsCollector statsCollector;
	Integrator integrator;
	static final Logger logger = LogManager.getLogger();
}
