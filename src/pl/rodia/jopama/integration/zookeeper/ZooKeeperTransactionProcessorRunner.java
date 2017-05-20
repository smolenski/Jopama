package pl.rodia.jopama.integration.zookeeper;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ZooKeeperTransactionProcessorRunner extends ZooKeeperSyncedRunner
{

	public ZooKeeperTransactionProcessorRunner(
			String addresses,
			Integer clusterSize,
			String startFinishDir,
			Integer clusterId,
			Integer numOutstanding
	)
	{
		super(addresses, clusterSize, startFinishDir);
		this.clusterId = clusterId;
		this.numOutstanding = numOutstanding;
	}

	void startDetected()
	{
		logger.info(
				"ZooKeeperTransactionProcessorRunner::start"
		);
		this.transactionProcessor = new ZooKeeperTransactionProcessor(
				this.addresses,
				this.clusterSize,
				this.clusterId,
				this.numOutstanding
		);
		this.transactionProcessor.start();
	}

	void finishDetected()
	{
		try
		{
			this.transactionProcessor.finish();
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
	Integer numOutstanding;
	ZooKeeperTransactionProcessor transactionProcessor;
	static final Logger logger = LogManager.getLogger();

	public static void main(
			String[] args
	)
	{
		assert (args.length == 5);
		String addresses = args[0];
		Integer clusterSize = new Integer(
				Integer.parseInt(
						args[1]
				)
		);
		String startFinishDir = args[2];
		Integer clusterId = Integer.parseInt(
				args[3]
		);
		Integer numOutstanding = Integer.parseInt(
				args[4]
		);
		ZooKeeperTransactionProcessorRunner transactionCreatorRunner = new ZooKeeperTransactionProcessorRunner(
				addresses,
				clusterSize,
				startFinishDir,
				clusterId,
				numOutstanding
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
