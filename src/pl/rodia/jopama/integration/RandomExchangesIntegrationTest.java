package pl.rodia.jopama.integration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.Function;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.TransactionComponent;
import pl.rodia.jopama.data.TransactionPhase;
import pl.rodia.jopama.gateway.RemoteStorageGateway;
import pl.rodia.jopama.stats.StatsAsyncSource;
import pl.rodia.jopama.stats.StatsCollector;

public class RandomExchangesIntegrationTest
{
	void performTest(
			UniversalStorageAccess storageAccess,
			RemoteStorageGateway storageGateway,
			Integer NUM_INTEGRATORS,
			Integer NUM_COMPONENTS,
			Integer NUM_COMPONENTS_IN_TRANSACTION,
			Integer NUM_TRANSACTIONS,
			Integer TRANSACTION_REPEAT_COUNT,
			Integer NUM_IN_FLIGHT_IN_INITIATOR,
			Integer SINGLE_COMPONENT_LIMIT,
			Integer DURATION_SEC
	) throws InterruptedException, ExecutionException
	{
		Integer COMPONENT_ID_BASE = new Integer(
				10000
		);
		Integer TRANSACTION_ID_BASE = new Integer(
				2000000
		);
		Map<Long, ObjectId> componentIds = new TreeMap<Long, ObjectId>();
		for (int i = 0; i < NUM_COMPONENTS; ++i)
		{
			Long longId = new Long(
					new Long(
							COMPONENT_ID_BASE + i
					)
			);
			ObjectId componentLongId = storageAccess.createComponent(
					longId,
					new ExtendedComponent(
							new Component(
									0,
									null,
									COMPONENT_ID_BASE + i,
									null
							),
							new Integer(
									0
							)
					)
			);
			componentIds.put(
					longId,
					componentLongId
			);
		}
		Long seed = new Long(
				System.currentTimeMillis()
		);
		logger.info(
				"Using seed: " + seed
		);
		Random random = new Random(
				seed
		);
		SortedSet<ObjectId> transactionIds = new TreeSet<ObjectId>();
		SortedMap<ObjectId, Transaction> allTransactions = new TreeMap<ObjectId, Transaction>();
		for (int it = 0; it < NUM_TRANSACTIONS; ++it)
		{
			Long transactionLongId = new Long(
					TRANSACTION_ID_BASE + it
			);
			TreeMap<ObjectId, TransactionComponent> transactionComponents = new TreeMap<ObjectId, TransactionComponent>();
			for (int ic = 0; ic < NUM_COMPONENTS_IN_TRANSACTION; ++ic)
			{
				Long componentLongId = new Long(
						COMPONENT_ID_BASE + random.nextInt(
								NUM_COMPONENTS
						)
				);
				transactionComponents.put(
						componentIds.get(
								componentLongId
						),
						new TransactionComponent(
								null,
								ComponentPhase.INITIAL
						)
				);
			}
			Function randomExchangeFunction = new RandomExchangeFunction(transactionLongId, seed + transactionLongId);
			Transaction transaction = new Transaction(
				TransactionPhase.INITIAL,
				transactionComponents,
				randomExchangeFunction
			);
			ObjectId transactionId = storageAccess.createTransaction(
					transactionLongId,
					new ExtendedTransaction(
							transaction,
							new Integer(
									0
							)
					)
			);
			allTransactions.put(transactionId, transaction);
			transactionIds.add(transactionId);
		}

		Map<Integer, SortedMap<ObjectId, Transaction>> integratorTransactions = new HashMap<Integer, SortedMap<ObjectId, Transaction>>();
		for (int ii = 0; ii < NUM_INTEGRATORS; ++ii)
		{
			integratorTransactions.put(
					new Integer(
							ii
					),
					new TreeMap<ObjectId, Transaction>()
			);
		}
		//for (int it = 0; it < NUM_TRANSACTIONS; ++it)
		for (ObjectId transactionId : transactionIds)
		{
			Set<Integer> integratorIds = new HashSet<Integer>();
			while (
				integratorIds.size() < TRANSACTION_REPEAT_COUNT
			)
			{
				integratorIds.add(
						random.nextInt(
								integratorTransactions.size()
						)
				);
			}
			for (Integer integratorId : integratorIds)
			{
				integratorTransactions.get(
						integratorId
				).put(
						transactionId,
						allTransactions.get(transactionId)
				);
			}
		}

		List<Integrator> integrators = new LinkedList<Integrator>();
		Long processingStartTimeMillis = System.currentTimeMillis();
		for (int ii = 0; ii < NUM_INTEGRATORS; ++ii)
		{
			SortedMap<ObjectId, Transaction> transactions = integratorTransactions.get(ii);
			Integrator integrator = new Integrator(
					"Integrator_" + ii,
					storageGateway,
					transactions,
					NUM_IN_FLIGHT_IN_INITIATOR,
					SINGLE_COMPONENT_LIMIT
			);
			integrators.add(
					integrator
			);
			integrator.start();
		}

		List<StatsAsyncSource> statsSources = new LinkedList<StatsAsyncSource>();
		for (Integrator integrator : integrators)
		{
			statsSources.addAll(
					integrator.getStatsSources()
			);
		}		
		StatsCollector statsCollector = new StatsCollector(
				statsSources
		);

		statsCollector.start();

		Thread.sleep(
				DURATION_SEC * 1000
		);

		logger.info(
				"Tearing down stats collector - prepareToFinish"
		);
		statsCollector.prepareToFinish();

		for (Integrator integrator : integrators)
		{
			logger.info(
					"Preparing to finish integrator: " + integrator.taskRunner.name
			);
			integrator.prepareToFinish();
		}
		for (Integrator integrator : integrators)
		{
			logger.info(
					"Finishing integrator: " + integrator.taskRunner.name
			);
			integrator.finish();
		}
		Long processingDurationMillis = System.currentTimeMillis() - processingStartTimeMillis;
		Long processingDurationSecs = processingDurationMillis / 1000;

		logger.info(
				"Tearing down stats collector - finish"
		);
		statsCollector.finish();

		Integer numFinished = new Integer(
				0
		);
		for (Integrator integrator : integrators)
		{
			numFinished += integrator.paceMaker.getNumFinished();
		}

		TreeMap<Integer, Boolean> valueExists = new TreeMap<Integer, Boolean>();
		for (int i = 0; i < NUM_COMPONENTS; ++i)
		{
			valueExists.put(
					COMPONENT_ID_BASE + i,
					new Boolean(
							false
					)
			);
		}
		logger.info(
				"FINAL STATE BEGIN"
		);
		for (int i = 0; i < NUM_COMPONENTS; ++i)
		{
			ObjectId componentId = componentIds.get(
					new Long(
							COMPONENT_ID_BASE + i
					)
			);
			int value = storageAccess.getComponent(
					componentId
			).component.value;
			assert valueExists.get(
					value
			) != null;
			valueExists.put(
					value,
					new Boolean(
							true
					)
			);
			logger.info(
					"ComponentId: " + componentId + " => " + value
			);
		}
		logger.info(
				"FINAL STATE END"
		);
		assert valueExists.size() == NUM_COMPONENTS;
		for (Boolean exists : valueExists.values())
		{
			assert exists.equals(
					new Boolean(
							true
					)
			);
		}

		logger.info(
				"Performed: " + numFinished + " tra "
						+ "in: " + processingDurationSecs + " sec "
						+ "("
						+ (new Double(
								numFinished
						) / TRANSACTION_REPEAT_COUNT / processingDurationSecs) + " tra/sec"
						+ ")"
		);

	}

	static final Logger logger = LogManager.getLogger();
}
