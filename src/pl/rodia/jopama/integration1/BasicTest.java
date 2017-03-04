package pl.rodia.jopama.integration1;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.Increment;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.TransactionComponent;
import pl.rodia.jopama.data.TransactionPhase;
import pl.rodia.jopama.stats.StatsAsyncSource;
import pl.rodia.jopama.stats.StatsCollector;

public class BasicTest
{
	public static void main(
			String[] args
	)
	{
		final int numIntegrators = 10;
		InMemoryStorageGateway inMemoryStorageGateway = new InMemoryStorageGateway();
		List<Integrator> integrators = new LinkedList<Integrator>();
		for (int i = 0; i < numIntegrators; ++i)
		{
			integrators.add(
					new Integrator(
							"Integrator_" + i,
							inMemoryStorageGateway
					)
			);
		}
		inMemoryStorageGateway.components.put(
				101,
				new Component(
						0,
						null,
						0,
						null
				)
		);
		inMemoryStorageGateway.components.put(
				102,
				new Component(
						0,
						null,
						0,
						null
				)
		);
		inMemoryStorageGateway.components.put(
				103,
				new Component(
						0,
						null,
						0,
						null
				)
		);
		TreeMap<Integer, TransactionComponent> transactionComponents = new TreeMap<Integer, TransactionComponent>();
		transactionComponents.put(
				101,
				new TransactionComponent(
						null,
						ComponentPhase.INITIAL
				)
		);
		transactionComponents.put(
				102,
				new TransactionComponent(
						null,
						ComponentPhase.INITIAL
				)
		);
		transactionComponents.put(
				103,
				new TransactionComponent(
						null,
						ComponentPhase.INITIAL
				)
		);
		inMemoryStorageGateway.transactions.put(
				1001,
				new Transaction(
						TransactionPhase.INITIAL,
						transactionComponents,
						new Increment()
				)
		);

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
			integrator.addTransaction(
					1001
			);
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

		logger.info(
				inMemoryStorageGateway.components.get(
						101
				).value
		);
		logger.info(
				inMemoryStorageGateway.components.get(
						102
				).value
		);
		logger.info(
				inMemoryStorageGateway.components.get(
						103
				).value
		);
	}

	static final Logger logger = LogManager.getLogger();

}
