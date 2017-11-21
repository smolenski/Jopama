package pl.rodia.jopama.integration.zookeeper;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ZooKeeperTransactionProcessorRunner extends ZooKeeperSyncedRunner
{

	public ZooKeeperTransactionProcessorRunner(
			String id,
			String addresses,
			Integer clusterSize,
			String startFinishDir,
			Integer clusterId,
			Integer numOutstanding
	)
	{
		super(id, addresses, clusterSize, startFinishDir);
		this.clusterId = clusterId;
		this.numOutstanding = numOutstanding;
	}

	void startDetected()
	{
		logger.info(this.id + " start detected - starting");
		this.transactionProcessor = new ZooKeeperTransactionProcessor(
				this.id + "TransactionProcessor",
				this.addresses,
				this.clusterSize,
				this.clusterId,
				this.numOutstanding
		);
		this.transactionProcessor.start();
		logger.info(this.id + " start detected - done");
	}

	void finishDetected()
	{
		logger.info(this.id + " finish detected - finishing");
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
		logger.info(this.id + " finish detected - done");
	}
	
	String getReadyPath()
	{
		return "/TP_READY/" + this.id;
	}
	
	String getDonePath()
	{
		return "/TP_DONE/" + this.id;
	}
	
	String getReadyString()
	{
		return "READY";
	}
	
	String getDoneString()
	{
		return "DONE";
	}

	Integer clusterId;
	Integer numOutstanding;
	ZooKeeperTransactionProcessor transactionProcessor;
	static final Logger logger = LogManager.getLogger();

	public static void main(
			String[] args
	)
	{
		assert (args.length == 6);
		String id = args[0];
		String addresses = args[1];
		Integer clusterSize = new Integer(
				Integer.parseInt(
						args[2]
				)
		);
		String startFinishDir = args[3];
		Integer clusterId = Integer.parseInt(
				args[4]
		);
		Integer numOutstanding = Integer.parseInt(
				args[5]
		);
		ThreadExceptionHandlerSetter.setHandler();
		ZooKeeperTransactionProcessorRunner transactionCreatorRunner = new ZooKeeperTransactionProcessorRunner(
				id,
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
