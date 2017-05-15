package pl.rodia.jopama.integration.zookeeper;

import java.util.List;
import java.util.Random;
import java.util.TreeMap;

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
import pl.rodia.mpf.Task;

public class ZooKeeperTransactionCreator extends ZooKeeperActorBase
{

	public ZooKeeperTransactionCreator(
			String addresses,
			Integer clusterSize,
			Integer desiredOutstandingTransactionsNum,
			Long firstComponentId,
			Long numComponents,
			Long numComponentsInTransaction
	)
	{
		super(
				addresses,
				clusterSize
		);
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
	}

	public Long getRetryDelay()
	{
		return new Long(
				1000
		);
	}

	public void tryToPerform()
	{
		ZooKeeperProvider zooKeeperProvider = zooKeeperMultiProvider.getResponsibleProvider(
				new Integer(
						this.random.nextInt()
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
				return;
			}

			zooKeeperProvider.zooKeeper.getChildren(
					ZooKeeperHelpers.getBasePath(),
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
												Integer numTransactions = new Integer(
														0
												);
												for (String fileName : children)
												{
													String transactionPrefix = "Transaction_";
													if (
														fileName.startsWith(
																transactionPrefix
														)
													)
													{
														numTransactions = new Integer(
																numTransactions + 1
														);
													}
												}
												tryToPerformCont(
														numTransactions
												);
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

	public Transaction generateTransaction(
			Long transactionId
	)
	{
		TreeMap<ObjectId, TransactionComponent> transactionComponents = new TreeMap<ObjectId, TransactionComponent>();
		while (
			transactionComponents.size() < this.numComponentsInTransaction
		)
		{
			Long componentId = this.firstComponentId + (this.random.nextLong() % this.numComponents);
			transactionComponents.put(
					new ZooKeeperObjectId(ZooKeeperObjectId.getComponentUniqueName(componentId)),
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
			Integer numExistingFiles
	)
	{
		logger.info(
				"numExisitingFiles: " + numExistingFiles
		);

		assert this.desiredOutstandingTransactionsNum.compareTo(
				numExistingFiles
		) >= 0;
		Integer numFilesToCreate = (this.desiredOutstandingTransactionsNum - numExistingFiles);
		logger.info(
				"ZooKeeperTransactionCreator::createFilesUpToTheTreshold, numFilesToCreate: " + numFilesToCreate
		);

		for (int i = 0; i < numFilesToCreate; ++i)
		{
			Long transactionId = this.random.nextLong();
			ZooKeeperObjectId zooKeeperObjectId = new ZooKeeperObjectId(
					ZooKeeperObjectId.getTransactionUniqueName(transactionId)
			);
			ZooKeeperProvider zooKeeperProvider = this.zooKeeperMultiProvider.getResponsibleProvider(
					zooKeeperObjectId.getClusterId(
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
					continue;
				}
				else
				{
					Transaction transaction = this.generateTransaction(
							transactionId
					);
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
									logger.debug(
											"ZooKeeperTransactionCreator fileCreated, name: " + zooKeeperObjectId.uniqueName
									);
								}
							},
							zooKeeperObjectId
					);
				}
			}
		}
	}

	Integer desiredOutstandingTransactionsNum;
	Long seed;
	Random random;
	Long firstComponentId;
	Long numComponents;
	Long numComponentsInTransaction;
	static final Logger logger = LogManager.getLogger();
}
