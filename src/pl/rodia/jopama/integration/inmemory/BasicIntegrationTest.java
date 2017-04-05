package pl.rodia.jopama.integration.inmemory;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.Increment;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.SimpleObjectId;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.TransactionComponent;
import pl.rodia.jopama.data.TransactionPhase;
import pl.rodia.jopama.stats.StatsAsyncSource;
import pl.rodia.jopama.stats.StatsCollector;

public class BasicIntegrationTest
{
	@Test
	public void singleTransactionConcurrentlyProcessedByManyIntegratorsTest() throws InterruptedException, ExecutionException
	{
		final int numIntegrators = 10;
		InMemoryStorageGateway inMemoryStorageGateway = new InMemoryStorageGateway();
		inMemoryStorageGateway.components.put(
				new SimpleObjectId(
						101
				),
				new ExtendedComponent(
						new Component(
								0,
								null,
								0,
								null
						),
						new Integer(
								0
						)
				)
		);
		inMemoryStorageGateway.components.put(
				new SimpleObjectId(
						102
				),
				new ExtendedComponent(
						new Component(
								0,
								null,
								0,
								null
						),
						new Integer(
								0
						)
				)
		);
		inMemoryStorageGateway.components.put(
				new SimpleObjectId(
						103
				),
				new ExtendedComponent(
						new Component(
								0,
								null,
								0,
								null
						),
						new Integer(
								0
						)
				)
		);
		TreeMap<ObjectId, TransactionComponent> transactionComponents = new TreeMap<ObjectId, TransactionComponent>();
		transactionComponents.put(
				new SimpleObjectId(
						101
				),
				new TransactionComponent(
						null,
						ComponentPhase.INITIAL
				)
		);
		transactionComponents.put(
				new SimpleObjectId(
						102
				),
				new TransactionComponent(
						null,
						ComponentPhase.INITIAL
				)
		);
		transactionComponents.put(
				new SimpleObjectId(
						103
				),
				new TransactionComponent(
						null,
						ComponentPhase.INITIAL
				)
		);
		inMemoryStorageGateway.transactions.put(
				new SimpleObjectId(
						1001
				),
				new ExtendedTransaction(
						new Transaction(
								TransactionPhase.INITIAL,
								transactionComponents,
								new Increment()
						),
						new Integer(
								0
						)
				)
		);

		List<ObjectId> transactionIds = new LinkedList<ObjectId>();
		transactionIds.add(
				new SimpleObjectId(
						1001
				)
		);

		List<Integrator> integrators = new LinkedList<Integrator>();
		for (int i = 0; i < numIntegrators; ++i)
		{
			integrators.add(
					new Integrator(
							"Integrator_" + i,
							inMemoryStorageGateway,
							transactionIds,
							transactionIds.size()
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

		statsCollector.start();

		statsCollector.prepareToFinish();

		for (Integrator integrator : integrators)
		{
			integrator.prepareToFinish();
		}
		for (Integrator integrator : integrators)
		{
			integrator.finish();
		}

		statsCollector.finish();

		ExtendedComponent component101 = inMemoryStorageGateway.components.get(
				new SimpleObjectId(
						101
				)
		);
		ExtendedComponent component102 = inMemoryStorageGateway.components.get(
				new SimpleObjectId(
						102
				)
		);
		ExtendedComponent component103 = inMemoryStorageGateway.components.get(
				new SimpleObjectId(
						102
				)
		);
		logger.info(
				component101.component.value + " (version:" + component101.externalVersion + ")"
		);
		logger.info(
				component102.component.value + " (version:" + component102.externalVersion + ")"
		);
		logger.info(
				component103.component.value + " (version:" + component103.externalVersion + ")"
		);
	}

	static final Logger logger = LogManager.getLogger();

}
