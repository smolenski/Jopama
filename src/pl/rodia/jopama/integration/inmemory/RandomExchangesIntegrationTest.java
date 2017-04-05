package pl.rodia.jopama.integration.inmemory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.Function;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.SimpleObjectId;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.TransactionComponent;
import pl.rodia.jopama.data.TransactionPhase;
import pl.rodia.jopama.stats.StatsAsyncSource;
import pl.rodia.jopama.stats.StatsCollector;

public class RandomExchangesIntegrationTest
{
	void performTest(
			Integer NUM_INTEGRATORS,
			Integer NUM_COMPONENTS,
			Integer NUM_COMPONENTS_IN_TRANSACTION,
			Integer NUM_TRANSACTIONS,
			Integer TRANSACTION_REPEAT_COUNT,
			Integer NUM_IN_FLIGHT_IN_INITIATOR,
			Integer DURATION_SEC
	) throws InterruptedException, ExecutionException
	{
		Integer COMPONENT_ID_BASE = new Integer(
				10000
		);
		Integer TRANSACTION_ID_BASE = new Integer(
				2000000
		);
		InMemoryStorageGateway inMemoryStorageGateway = new InMemoryStorageGateway();
		for (int i = 0; i < NUM_COMPONENTS; ++i)
		{
			inMemoryStorageGateway.components.put(
					new SimpleObjectId(COMPONENT_ID_BASE + i),
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
		for (int it = 0; it < NUM_TRANSACTIONS; ++it)
		{
			ObjectId transactionId = new SimpleObjectId(
					TRANSACTION_ID_BASE + it
			);
			TreeMap<ObjectId, TransactionComponent> transactionComponents = new TreeMap<ObjectId, TransactionComponent>();
			for (int ic = 0; ic < NUM_COMPONENTS_IN_TRANSACTION; ++ic)
			{
				transactionComponents.put(
						new SimpleObjectId(
								COMPONENT_ID_BASE + random.nextInt(
										NUM_COMPONENTS
								)
						),
						new TransactionComponent(
								null,
								ComponentPhase.INITIAL
						)
				);
			}
			Function randomExchangeFunction = new Function()
			{
				@Override
				public Map<ObjectId, Integer> execute(
						Map<ObjectId, Integer> oldValues
				)
				{
					Random random = new Random(
							seed + transactionId.toLong()
					);
					Map<ObjectId, Integer> result = new TreeMap<ObjectId, Integer>();
					for (Map.Entry<ObjectId, Integer> entry : oldValues.entrySet())
					{
						result.put(
								entry.getKey(),
								new Integer(
										entry.getValue()
								)
						);
					}
					if (
						result.size() <= 1
					)
					{
						return result;
					}
					for (int i = 0; i < result.size(); ++i)
					{
						int indexToExchangeWith = random.nextInt(
								oldValues.size() - 1
						);
						if (
							indexToExchangeWith >= i
						)
						{
							++indexToExchangeWith;
						}
						assert indexToExchangeWith != i;
						ObjectId keyExchange1 = null;
						ObjectId keyExchange2 = null;
						int vi = 0;
						for (Map.Entry<ObjectId, Integer> entry : result.entrySet())
						{
							if (
								vi == i
							)
							{
								keyExchange1 = entry.getKey();
							}
							if (
								vi == indexToExchangeWith
							)
							{
								keyExchange2 = entry.getKey();
							}
							if (
								keyExchange1 != null && keyExchange2 != null
							)
							{
								break;
							}
							++vi;

						}
						assert keyExchange1 != null;
						assert keyExchange2 != null;
						Integer valueExchange1 = new Integer(
								result.get(
										keyExchange1
								)
						);
						Integer valueExchange2 = new Integer(
								result.get(
										keyExchange2
								)
						);
						logger.debug(
								"Exchanging: " + valueExchange1 + " <=> " + valueExchange2
						);
						result.put(
								keyExchange1,
								valueExchange2
						);
						result.put(
								keyExchange2,
								valueExchange1
						);
					}
					return result;
				}
			};
			inMemoryStorageGateway.transactions.put(
					transactionId,
					new ExtendedTransaction(
							new Transaction(
									TransactionPhase.INITIAL,
									transactionComponents,
									randomExchangeFunction
							),
							new Integer(
									0
							)
					)
			);
		}

		List<StatsAsyncSource> statsSources = new LinkedList<StatsAsyncSource>();

		Map<Integer, List<ObjectId>> integratorTransactions = new HashMap<Integer, List<ObjectId>>();
		for (int ii = 0; ii < NUM_INTEGRATORS; ++ii)
		{
			integratorTransactions.put(
					new Integer(
							ii
					),
					new LinkedList<ObjectId>()
			);
		}
		for (int it = 0; it < NUM_TRANSACTIONS; ++it)
		{
			ObjectId transactionId = new SimpleObjectId(
					TRANSACTION_ID_BASE + it
			);
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
				).add(
						transactionId
				);
			}
		}

		List<Integrator> integrators = new LinkedList<Integrator>();
		Long processingStartTimeMillis = System.currentTimeMillis();
		for (int ii = 0; ii < NUM_INTEGRATORS; ++ii)
		{
			List<ObjectId> transactions = integratorTransactions.get(
					ii
			);
			Integrator integrator = new Integrator(
					"Integrator_" + ii,
					inMemoryStorageGateway,
					transactions,
					NUM_IN_FLIGHT_IN_INITIATOR
			);
			integrators.add(
					integrator
			);
			integrator.start();
		}

		for (Integrator integrator : integrators)
		{
			StatsAsyncSource taskRunnerStatsSource = new StatsAsyncSource(
					integrator.taskRunner,
					integrator.taskRunner
			);
			statsSources.add(
					taskRunnerStatsSource
			);
			StatsAsyncSource remoteStorageGatewayStatsSource = new StatsAsyncSource(
					integrator.taskRunner,
					integrator.remoteStorageGateway
			);
			statsSources.add(
					remoteStorageGatewayStatsSource
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
		logger.debug(
				"FINAL STATE BEGIN"
		);
		for (int i = 0; i < NUM_COMPONENTS; ++i)
		{
			int value = inMemoryStorageGateway.components.get(
					new SimpleObjectId(COMPONENT_ID_BASE + i)
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
			logger.debug(
					"Component: " + (COMPONENT_ID_BASE + i) + " => " + value
			);
		}
		logger.debug(
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
