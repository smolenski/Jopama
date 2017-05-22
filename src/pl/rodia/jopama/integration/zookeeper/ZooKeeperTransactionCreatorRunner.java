package pl.rodia.jopama.integration.zookeeper;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ZooKeeperTransactionCreatorRunner extends ZooKeeperSyncedRunner
{

	public ZooKeeperTransactionCreatorRunner(
			String addresses,
			Integer clusterSize,
			String startFinishDir,
			Integer clusterId,
			Integer desiredOutstandingTransactionsNum,
			Long firstComponentId,
			Long numComponents,
			Long numComponentsInTransaction
	)
	{
		super(addresses, clusterSize, startFinishDir);
		this.clusterId = clusterId;
		this.desiredOutstandingTransactionsNum = desiredOutstandingTransactionsNum;
		this.firstComponentId = firstComponentId;
		this.numComponents = numComponents;
		this.numComponentsInTransaction = numComponentsInTransaction;
	}

	void startDetected()
	{
		logger.info(
				"ZooKeeperTransactionCreatorRunner::start"
		);
		this.transactionCreator = new ZooKeeperTransactionCreator(
				this.addresses,
				this.clusterSize,
				this.clusterId,
				this.desiredOutstandingTransactionsNum,
				this.firstComponentId,
				this.numComponents,
				this.numComponentsInTransaction
		);
		this.transactionCreator.start();
	}

	void finishDetected()
	{
		try
		{
			this.transactionCreator.finish();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		catch (ExecutionException e)
		{
			e.printStackTrace();
		}
		this.finish();
	}

	Integer clusterId;
	Integer desiredOutstandingTransactionsNum;
	Long firstComponentId;
	Long numComponents;
	Long numComponentsInTransaction;
	ZooKeeperTransactionCreator transactionCreator;
	static final Logger logger = LogManager.getLogger();

	public static void main(
			String[] args
	)
	{
		assert (args.length == 8);
		String addresses = args[0];
		Integer clusterSize = new Integer(
				Integer.parseInt(
						args[1]
				)
		);
		String startFinishDir = args[2];
		Integer clusterId = new Integer(
				Integer.parseInt(
						args[3]
				)
		);
		Integer desiredOutstandingTransactionsNum = Integer.parseInt(
				args[4]
		);
		Long firstComponentId = Long.parseLong(
				args[5]
		);
		Long numComponents = Long.parseLong(
				args[6]
		);
		Long numComponentsInTransaction = Long.parseLong(
				args[7]
		);
		ZooKeeperTransactionCreatorRunner transactionCreatorRunner = new ZooKeeperTransactionCreatorRunner(
				addresses,
				clusterSize,
				startFinishDir,
				clusterId,
				desiredOutstandingTransactionsNum,
				firstComponentId,
				numComponents,
				numComponentsInTransaction
		);
		transactionCreatorRunner.start();
		try
		{
			transactionCreatorRunner.waitForFinish();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}
}
