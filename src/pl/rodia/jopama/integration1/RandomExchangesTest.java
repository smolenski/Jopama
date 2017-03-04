package pl.rodia.jopama.integration1;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.Function;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.TransactionComponent;
import pl.rodia.jopama.data.TransactionPhase;
import pl.rodia.jopama.stats.StatsAsyncSource;
import pl.rodia.jopama.stats.StatsCollector;

public class RandomExchangesTest
{
	public static void main(
			String[] args
	)
	{
		InMemoryStorageGateway inMemoryStorageGateway = new InMemoryStorageGateway();
		List<Integrator> integrators = new LinkedList<Integrator>();
		for (int i = 0; i < NUM_INTEGRATORS; ++i)
		{
			integrators.add(
					new Integrator(
							"Integrator_" + i,
							inMemoryStorageGateway
					)
			);
		}
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
					if (result.size() <= 1)
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
									keyExchange1 = new Integer(entry.getKey());
								}
								if (
									vi == indexToExchangeWith
								)
								{
									keyExchange2 = new Integer(entry.getKey());
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
						Integer valueExchange1 = new Integer(result.get(keyExchange1));
						Integer valueExchange2 = new Integer(result.get(keyExchange2));
						logger.debug("Exchanging: " + valueExchange1 + " <=> " + valueExchange2);
						result.put(keyExchange1, valueExchange2);
						result.put(keyExchange2, valueExchange1);
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

		for (Integrator integrator : integrators)
		{
			integrator.start();
		}
		for (int it = 0; it < NUM_TRANSACTIONS; ++it)
		{
			Integer transactionId = new Integer(
					TRANSACTION_ID_BASE + it
			);
			for (int ic = 0; ic < TRANSACTION_REPEAT_COUNT; ++ic)
			{
				Integrator integrator = integrators.get(
						random.nextInt(
								integrators.size()
						)
				);
				integrator.addTransaction(
						transactionId
				);
			}
		}

		statsCollector.start();

		for (Integrator integrator : integrators)
		{
			try
			{
				integrator.waitUntilTransactionProcessingFinished();
			}
			catch (InterruptedException e1)
			{
				e1.printStackTrace();
			}
		}

		for (Integrator integrator : integrators)
		{
			try
			{
				integrator.teardown();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}

		try
		{
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
			assert valueExists.get(value) != null;
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
/*
	static final Integer COMPONENT_ID_BASE = 100;
	static final Integer TRANSACTION_ID_BASE = 1000;
	static final Integer NUM_INTEGRATORS = 10;
	static final Integer NUM_COMPONENTS = 100;
	static final Integer NUM_COMPONENTS_IN_TRANSACTION = 10;
	static final Integer NUM_TRANSACTIONS = 10;
	static final Integer TRANSACTION_REPEAT_COUNT = 3;
*/
	static final Integer COMPONENT_ID_BASE = 100;
	static final Integer TRANSACTION_ID_BASE = 1000;
	static final Integer NUM_INTEGRATORS = 10;
	static final Integer NUM_COMPONENTS = 100;
	static final Integer NUM_COMPONENTS_IN_TRANSACTION = 10;
	static final Integer NUM_TRANSACTIONS = 10;
	static final Integer TRANSACTION_REPEAT_COUNT = 2;

	static final Logger logger = LogManager.getLogger();

}
