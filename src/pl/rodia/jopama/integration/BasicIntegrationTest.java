package pl.rodia.jopama.integration;

import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.ComponentPhase;
import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.Increment;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.TransactionComponent;
import pl.rodia.jopama.data.TransactionPhase;
import pl.rodia.jopama.gateway.RemoteStorageGateway;
import pl.rodia.jopama.stats.StatsAsyncSource;
import pl.rodia.jopama.stats.StatsCollector;

public class BasicIntegrationTest
{

	public void singleTransactionConcurrentlyProcessedByManyIntegratorsTest(
			UniversalStorageAccess storageAccess,
			RemoteStorageGateway storageGateway
	) throws InterruptedException, ExecutionException
	{
		final int numIntegrators = 10;
		ObjectId comp1 = storageAccess.createComponent(
				new Long(
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
		ObjectId comp2 = storageAccess.createComponent(
				new Long(
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
		ObjectId comp3 = storageAccess.createComponent(
				new Long(
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
				comp1,
				new TransactionComponent(
						null,
						ComponentPhase.INITIAL
				)
		);
		transactionComponents.put(
				comp2,
				new TransactionComponent(
						null,
						ComponentPhase.INITIAL
				)
		);
		transactionComponents.put(
				comp3,
				new TransactionComponent(
						null,
						ComponentPhase.INITIAL
				)
		);
		Transaction transaction1 = new Transaction(
			TransactionPhase.INITIAL,
			transactionComponents,
			new Increment()
		);
		ObjectId tranId1 = storageAccess.createTransaction(
				new Long(
						1001
				),
				new ExtendedTransaction(
						transaction1,
						new Integer(
								0
						)
				)
		);

		SortedMap<ObjectId, Transaction> transactionIds = new TreeMap<ObjectId, Transaction>();
		transactionIds.put(
				tranId1,
				transaction1
		);

		List<Integrator> integrators = new LinkedList<Integrator>();
		for (int i = 0; i < numIntegrators; ++i)
		{
			integrators.add(
					new Integrator(
							"Integrator_" + i,
							storageGateway,
							transactionIds,
							transactionIds.size(),
							transactionIds.size()
					)
			);
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

		ExtendedComponent component101 = storageAccess.getComponent(
				comp1
		);
		ExtendedComponent component102 = storageAccess.getComponent(
				comp2
		);
		ExtendedComponent component103 = storageAccess.getComponent(
				comp3
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
