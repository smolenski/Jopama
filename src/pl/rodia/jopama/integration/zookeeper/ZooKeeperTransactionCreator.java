package pl.rodia.jopama.integration.zookeeper;

import java.util.List;

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
			String connectionString, String transactionDir, Integer desiredOutstandingTransactionsNum, Integer numCreators
	)
	{
		super(
				connectionString
		);
		this.transactionDir = transactionDir;
		this.desiredOutstandingTransactionsNum = desiredOutstandingTransactionsNum;
	}

	public Long getRetryDelay()
	{
		return new Long(
				3000
		);
	}

	public void tryToPerform()
	{
		this.zooKeeperProvider.zooKeeper.getChildren(
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

	public void tryToPerformCont(
			Integer numExistingFiles
	)
	{
		synchronized (this.zooKeeperProvider)
		{
			if (
				this.zooKeeperProvider.zooKeeper == null
						||
						this.zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
			)
			{
				return;
			}
			else
			{
				assert this.desiredOutstandingTransactionsNum.compareTo(
						numExistingFiles
				) >= 0;
				Integer numFilesToCreate = (this.desiredOutstandingTransactionsNum - numExistingFiles);
				logger.info(
						"ZooKeeperTransactionCreator::createFilesUpToTheTreshold, numFilesToCreate: " + numFilesToCreate
				);
				for (int i = 0; i < numFilesToCreate.intValue(); ++i)
				{
					byte[] serializedTransaction = new byte[10];
					this.zooKeeperProvider.zooKeeper.create(
							this.transactionDir + "/",
							serializedTransaction,
							Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT_SEQUENTIAL,
							new StringCallback()
							{
								@Override
								public void processResult(
										int rc, String path, Object ctx, String name
								)
								{
									logger.debug(
											"ZooKeeperTransactionCreator fileCreated, i: " + ctx
									);
								}
							},
							new Integer(
									i
							)
					);
				}
			}
		}
	}

	String connectionString;
	String transactionDir;
	Integer desiredOutstandingTransactionsNum;
	static final Logger logger = LogManager.getLogger();
}
