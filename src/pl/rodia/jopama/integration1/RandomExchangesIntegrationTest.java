package pl.rodia.jopama.integration1;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.Function;
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
			Integer TRANSACTION_REPEAT_COUNT
	)
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
					COMPONENT_ID_BASE + i,
					new Component(
							0,
							null,
							COMPONENT_ID_BASE + i,
							null
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
			Integer transactionId = new Integer(
					TRANSACTION_ID_BASE + it
			);
			TreeMap<Integer, TransactionComponent> transactionComponents = new TreeMap<Integer, TransactionComponent>();
			for (int ic = 0; ic < NUM_COMPONENTS_IN_TRANSACTION; ++ic)
			{
				transactionComponents.put(
						COMPONENT_ID_BASE + random.nextInt(
								NUM_COMPONENTS
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
				public Map<Integer, Integer> execute(
						Map<Integer, Integer> oldValues
				)
				{
					Random random = new Random(
							seed + transactionId
					);
					Map<Integer, Integer> result = new TreeMap<Integer, Integer>();
					for (Map.Entry<Integer, Integer> entry : oldValues.entrySet())
					{
						result.put(
								new Integer(
										entry.getKey()
								),
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
						Integer keyExchange1 = null;
						Integer keyExchange2 = null;
						int vi = 0;
						for (Map.Entry<Integer, Integer> entry : result.entrySet())
						{
							if (
								vi == i
							)
							{
								keyExchange1 = new Integer(
										entry.getKey()
								);
							}
							if (
								vi == indexToExchangeWith
							)
							{
								keyExchange2 = new Integer(
										entry.getKey()
								);
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
					new Integer(
							transactionId
					),
					new Transaction(
							TransactionPhase.INITIAL,
							transactionComponents,
							randomExchangeFunction
					)
			);
		}

		List<StatsAsyncSource> statsSources = new LinkedList<StatsAsyncSource>();

		Map<Integer, List<Integer>> integratorTransactions = new HashMap<Integer, List<Integer>>();
		for (int ii = 0; ii < NUM_INTEGRATORS; ++ii)
		{
			integratorTransactions.put(
					new Integer(
							ii
					),
					new LinkedList<Integer>()
			);
		}
		for (int it = 0; it < NUM_TRANSACTIONS; ++it)
		{
			Integer transactionId = new Integer(
					TRANSACTION_ID_BASE + it
			);
			for (int ic = 0; ic < TRANSACTION_REPEAT_COUNT; ++ic)
			{
				Integer integratorId = new Integer(
						random.nextInt(
								integratorTransactions.size()
						)
				);
				integratorTransactions.get(
						integratorId
				).add(
						transactionId
				);
			}
		}

		List<Integrator> integrators = new LinkedList<Integrator>();
		for (int ii = 0; ii < NUM_INTEGRATORS; ++ii)
		{
			List<Integer> transactions = integratorTransactions.get(ii);
			Integrator integrator = new Integrator("Integrator_" + ii, inMemoryStorageGateway, transactions, transactions.size());
			integrators.add(integrator);
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
		
		for (Integrator integrator : integrators)
		{
			try
			{
				logger.info("Tearing down integrator: " + integrator.taskRunner.name);
				integrator.teardown();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			catch (ExecutionException e)
			{
				e.printStackTrace();
			}
		}

		try
		{
			logger.info("Tearing down stats collector");
			statsCollector.teardown();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
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
					COMPONENT_ID_BASE + i
			).value;
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

	}

	@Test
	public void conflictingAndNotConflictingTransactions()
	{
		this.performTest(
				10,
				100,
				10,
				30,
				2
		);
	}

	static final Logger logger = LogManager.getLogger();
}
