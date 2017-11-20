package pl.rodia.jopama.integration.zookeeper;

import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ZooKeeperTransactionCreatorRunner extends ZooKeeperSyncedRunner
{

	public ZooKeeperTransactionCreatorRunner(
			String id,
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
		super(id, addresses, clusterSize, startFinishDir);
		this.clusterId = clusterId;
		this.desiredOutstandingTransactionsNum = desiredOutstandingTransactionsNum;
		this.firstComponentId = firstComponentId;
		this.numComponents = numComponents;
		this.numComponentsInTransaction = numComponentsInTransaction;
	}

	void startDetected()
	{
		logger.info(this.id + " start detected - starting");
		this.transactionCreator = new ZooKeeperTransactionCreator(
				this.id + "TransactionCreator",
				this.addresses,
				this.clusterSize,
				this.clusterId,
				this.desiredOutstandingTransactionsNum,
				this.firstComponentId,
				this.numComponents,
				this.numComponentsInTransaction
		);
		this.transactionCreator.start();
		logger.info(this.id + " start detected - done");
	}

	void finishDetected()
	{
		logger.info(this.id + " finish detected - finishting");
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
		logger.info(this.id + " finish detected - done");
	}
	
	String getReadyPath()
	{
		return "/TC_READY/" + this.id;
	}
	
	String getDonePath()
	{
		return "/TC_DONE/" + this.id;
	}
	
	String getReadyString()
	{
		return "READY";
	}
	
	String getDoneString()
	{
		Long value = this.transactionCreator.numCreated;
		return "DONE " + value.toString();
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
		assert (args.length == 9);
		String id = args[0];
		String addresses = args[1];
		Integer clusterSize = new Integer(
				Integer.parseInt(
						args[2]
				)
		);
		String startFinishDir = args[3];
		Integer clusterId = new Integer(
				Integer.parseInt(
						args[4]
				)
		);
		Integer desiredOutstandingTransactionsNum = Integer.parseInt(
				args[5]
		);
		Long firstComponentId = Long.parseLong(
				args[6]
		);
		Long numComponents = Long.parseLong(
				args[7]
		);
		Long numComponentsInTransaction = Long.parseLong(
				args[8]
		);
		ZooKeeperTransactionCreatorRunner transactionCreatorRunner = new ZooKeeperTransactionCreatorRunner(
				id,
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
