package pl.rodia.jopama.integration.zookeeper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.TransactionComponent;
import pl.rodia.jopama.data.TransactionPhase;
import pl.rodia.jopama.integration.RandomExchangeFunction;
import pl.rodia.jopama.integration.ZooKeeperTransactionHelpers;
import pl.rodia.mpf.Task;

public class ZooKeeperTransactionCreator extends ZooKeeperActorBase
{

	public ZooKeeperTransactionCreator(
			String id,
			String addresses,
			Integer clusterSize,
			Integer clusterId,
			Integer desiredOutstandingTransactionsNum,
			Long firstComponentId,
			Long numComponents,
			Long numComponentsInTransaction,
			Long singleComponentLimit
	)
	{
		super(
				id,
				addresses,
				clusterSize
		);
		this.clusterId = clusterId;
		this.desiredOutstandingTransactionsNum = desiredOutstandingTransactionsNum;
		this.seed = new Long(
				System.currentTimeMillis()
		);
		logger.info(
				"ZooKeeperTransactionCreator, seed: " + this.seed
		);
		this.random = new Random(
				this.seed
		);
		this.firstComponentId = firstComponentId;
		this.numComponents = numComponents;
		this.numComponentsInTransaction = numComponentsInTransaction;
		this.singleComponentLimit = singleComponentLimit;
		this.currentTransactions = new HashMap<Long, Transaction>();
        this.numAtomicAsyncOperations = new AtomicInteger(0);
		this.numCreated = new Long(0);
	}

	public Long getRetryDelay()
	{
		return new Long(
				2000
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

	public Transaction generateTransaction(
			Long transactionId,
			SortedMap<Long, Long> compsCount
	)
	{
		Long numUnusable = ZooKeeperTransactionHelpers.getNumUnusableComponents(this.singleComponentLimit, compsCount);
		assert (numComponents.compareTo(numUnusable) >= 0);
		Long usableComps = new Long(numComponents - numUnusable);
		if (usableComps.compareTo(this.numComponentsInTransaction) < 0)
		{
			return null;
		}
		TreeMap<ObjectId, TransactionComponent> transactionComponents = new TreeMap<ObjectId, TransactionComponent>();
		while (
			transactionComponents.size() < this.numComponentsInTransaction
		)
		{
			Long componentId = ZooKeeperTransactionHelpers.generateComponentId(
				this.firstComponentId.longValue(),
				this.numComponents.longValue(),
				this.singleComponentLimit.longValue(),
				this.random.nextLong(),
				compsCount
			);
			transactionComponents.put(
					new ZooKeeperObjectId(
							ZooKeeperObjectId.getComponentUniqueName(
									componentId
							)
					),
					new TransactionComponent(
							null,
							ComponentPhase.INITIAL
					)
			);
		}
		Transaction transaction = new Transaction(
				TransactionPhase.INITIAL,
				transactionComponents,
				new RandomExchangeFunction(
						transactionId,
						this.random.nextLong()
				)
		);
		return transaction;
	}

	public void tryToPerformCont(
			List<String> children
	)
	{
		Integer numExistingFiles = children.size();
        assert this.numAtomicAsyncOperations.get() == 1;
		logger.info(
				"numExisitingFiles: " + numExistingFiles
		);
		assert this.desiredOutstandingTransactionsNum.compareTo(
				numExistingFiles
		) >= 0;
		ZooKeeperProvider zooKeeperProvider = this.zooKeeperMultiProvider.getResponsibleProvider(
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
                Integer numAsyncBefore = new Integer(this.numAtomicAsyncOperations.getAndDecrement());
                assert numAsyncBefore.compareTo(new Integer(0)) > 0;
				return;
			}
			else
			{
				Map<Long, Transaction> transToCreate = this.generateNewTransactions(children);
				numAtomicAsyncOperations.set(transToCreate.size());
				for (Map.Entry<Long, Transaction> entry : transToCreate.entrySet())
				{
					Long transactionId = entry.getKey();
					ZooKeeperObjectId zooKeeperObjectId = new ZooKeeperObjectId(
							ZooKeeperObjectId.getTransactionUniqueName(
									transactionId
							)
					);
					assert zooKeeperObjectId.getClusterId(this.zooKeeperMultiProvider.getNumClusters()).equals(new Integer(this.clusterId));
					Transaction transaction = entry.getValue();
					byte[] serializedTransaction = ZooKeeperHelpers.serializeTransaction(transaction);
					zooKeeperProvider.zooKeeper.create(
							ZooKeeperHelpers.getTransactionPath(
									zooKeeperObjectId
							),
							serializedTransaction,
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
										logger.debug(
												"ZooKeeperTransactionCreator fileCreation success, name: " + zooKeeperObjectId.uniqueName
										);
										schedule(
											new Task()
											{
												@Override
												public void execute()
												{
													numCreated = new Long(numCreated.longValue() + 1);
													Transaction putResult = currentTransactions.put(transactionId, transaction);
													assert (putResult == null);
													Integer numAsyncBefore = new Integer(numAtomicAsyncOperations.getAndDecrement());
				                                    assert numAsyncBefore.compareTo(new Integer(0)) > 0;
												}
											}
										);
									}
									else
									{
										logger.debug(
												"ZooKeeperTransactionCreator fileCreation failed, name: " + zooKeeperObjectId.uniqueName
										);
										Integer numAsyncBefore = new Integer(numAtomicAsyncOperations.getAndDecrement());
	                                    assert numAsyncBefore.compareTo(new Integer(0)) > 0;
									}
								}
							},
							zooKeeperObjectId
					);
				}
			}
		}
	}
	

	Map<Long, Transaction> generateNewTransactions(List<String> existing)
	{
		Set<Long> existingIds = new HashSet<Long>();
		for (String name : existing)
		{
			ZooKeeperObjectId objectId = new ZooKeeperObjectId(name);
			assert (existingIds.add(objectId.getId()));
		}
		List<Long> tIdsToRemove = new LinkedList<Long>();
		for (Long tid : this.currentTransactions.keySet())
		{
			if (!existingIds.contains(tid))
			{
				tIdsToRemove.add(tid);
			}
		}
		for (Long tId : tIdsToRemove)
		{
			Transaction removeResult = this.currentTransactions.remove(tId);
			assert (removeResult != null);
		}
		SortedMap<Long, Long> compsCount = new TreeMap<Long, Long>();
		for (Map.Entry<Long, Transaction> entry : this.currentTransactions.entrySet())
		{
			ZooKeeperTransactionHelpers.updateCompCount(compsCount, entry.getValue());
		}
		Map<Long, Transaction> result = new HashMap<Long, Transaction>();
		while (this.currentTransactions.size() + result.size() < this.desiredOutstandingTransactionsNum)
		{
			Long transactionId = ZooKeeperObjectId.getRandomIdForCluster(
					this.random,
					this.clusterId,
					this.zooKeeperMultiProvider.getNumClusters()
			);
			Transaction transaction = this.generateTransaction(transactionId, compsCount);
			if (transaction == null)
			{
				break;
			}
			result.put(transactionId, transaction);
			ZooKeeperTransactionHelpers.updateCompCount(compsCount, transaction);
		}
		return result;
	}
	

	Integer clusterId;
	Integer desiredOutstandingTransactionsNum;
	Long seed;
	Random random;
	Long firstComponentId;
	Long numComponents;
	Long numComponentsInTransaction;
	Long singleComponentLimit;
	Map<Long, Transaction> currentTransactions;
	Long numCreated;
    AtomicInteger numAtomicAsyncOperations;
	static final Logger logger = LogManager.getLogger();
}
