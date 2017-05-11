package pl.rodia.jopama.integration.zookeeper;

import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import pl.rodia.mpf.Task;

public class ZooKeeperTransactionCreator extends ZooKeeperActorBase
{

	public ZooKeeperTransactionCreator(
			String addresses, Integer numClusters, String transactionDir, Integer desiredOutstandingTransactionsNum, Integer numCreators
	)
	{
		super(
				addresses,
				numClusters
		);
		this.transactionDir = transactionDir;
		this.desiredOutstandingTransactionsNum = desiredOutstandingTransactionsNum;
		this.random = new Random();
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
					this.transactionDir,
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
												tryToPerformCont(
														children.size()
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

			ZooKeeperObjectId zooKeeperObjectId = new ZooKeeperObjectId(
					String.format(
							"Transaction_%020d",
							Math.abs(
									this.random.nextLong()
							)
					)
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
					byte[] serializedTransaction = new byte[10];
					zooKeeperProvider.zooKeeper.create(
							this.transactionDir + "/" + zooKeeperObjectId.uniqueName,
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

	String connectionString;
	String transactionDir;
	Integer desiredOutstandingTransactionsNum;
	Random random;
	static final Logger logger = LogManager.getLogger();
}
