package pl.rodia.jopama.integration.zookeeper;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException.OperationTimeoutException;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.Increment;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.TransactionComponent;
import pl.rodia.jopama.data.TransactionPhase;

public class ZooKeeperCreatorSample
{
	public static void main(
			String[] args
	) throws InterruptedException, OperationTimeoutException
	{
		logger.debug(
				"args.length: " + args.length
		);
		if (
			args.length != 2
		)
		{
			throw new IllegalArgumentException(
					"Unexpected number of arguments: " + args.length
			);
		}

		ZooKeeperMultiProvider zooKeeperMultiProvider = new ZooKeeperMultiProvider(
				args[0],
				Integer.parseInt(
						args[1]
				)
		);
		zooKeeperMultiProvider.start();
		ZooKeeperStorageAccess zooKeeperCreator = new ZooKeeperStorageAccess(
				zooKeeperMultiProvider
		);
		Integer numComponents = 5;
		Integer numTransactions = 5;
		Component component = new Component(
				new Integer(
						0
				),
				null,
				new Integer(
						0
				),
				null
		);
		Long time = System.currentTimeMillis();
		Random random = new Random(
				time
		);
		Integer base = random.nextInt();
		if (
			base < 0
		)
		{
			base = -base;
		}
		List<ZooKeeperObjectId> componentIds = new LinkedList<ZooKeeperObjectId>();
		for (int i = 0; i < numComponents; ++i)
		{
			ZooKeeperObjectId componentId = new ZooKeeperObjectId(
					String.format(
							"Component_%010d_%010d",
							base,
							i
					)
			);
			logger.info(
					"Creating component: " + i + " componentId: " + componentId
			);
			componentIds.add(
					componentId
			);
			ZooKeeperObjectId result = zooKeeperCreator.createObject(
					componentId,
					ZooKeeperHelpers.serializeComponent(
							component
					)
			);
			logger.info(
					"Component created: " + result
			);
		}
		for (int i = 0; i < numTransactions; ++i)
		{
			TreeMap<ObjectId, TransactionComponent> transactionComponents = new TreeMap<ObjectId, TransactionComponent>();
			for (
					int tci = 0; tci < random.nextInt(
							componentIds.size()
					) + 1; ++tci
			)
			{
				transactionComponents.put(
						componentIds.get(
								random.nextInt(
										componentIds.size()
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
					new Increment()
			);
			ZooKeeperObjectId transactionId = new ZooKeeperObjectId(
					String.format(
							"Transaction_%010d_%010d",
							base,
							i
					)
			);
			logger.info(
					"Creating transaction: " + i + " transactionId: " + transactionId + " numComponents: " + transactionComponents.size()
			);
			ZooKeeperObjectId result = zooKeeperCreator.createObject(
					transactionId,
					ZooKeeperHelpers.serializeTransaction(
							transaction
					)
			);
			logger.info(
					"Component created: " + result
			);
		}
		zooKeeperMultiProvider.finish();
	}

	static final Logger logger = LogManager.getLogger();
}
