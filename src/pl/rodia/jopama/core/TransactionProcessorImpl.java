package pl.rodia.jopama.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.Transaction;
import pl.rodia.jopama.data.UnifiedAction;
import pl.rodia.jopama.gateway.ErrorCode;
import pl.rodia.jopama.gateway.NewComponentVersionFeedback;
import pl.rodia.jopama.gateway.NewTransactionVersionFeedback;
import pl.rodia.jopama.gateway.RemoteStorageGateway;
import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class TransactionProcessorImpl extends TransactionProcessor
{

	public TransactionProcessorImpl(
			TaskRunner taskRunner,
			TransactionAnalyzer transactionAnalyzer,
			LocalStorage storage,
			RemoteStorageGateway storageGateway
	)
	{
		super();
		this.taskRunner = taskRunner;
		this.transactionAnalyzer = transactionAnalyzer;
		this.storage = storage;
		this.storageGateway = storageGateway;
		this.transactions = new HashMap<Integer, Task>();
		this.scheduledProcessingTaskId = null;
		this.scheduleProcessing();
	}

	public void addTransaction(
			Integer transactionId,
			Task transactionDone
	)
	{
		this.transactions.put(
				transactionId,
				transactionDone
		);
		if (
			this.scheduledProcessingTaskId == null
		)
		{
			this.scheduleProcessing();
		}
		this.processTransaction(
				transactionId
		);
	}

	void removeTransaction(
			Integer transactionId
	)
	{
		Task task = this.transactions.remove(
				transactionId
		);
		if (
			task != null
		)
		{
			if (
				this.transactions.isEmpty()
			)
			{
				if (
					this.taskRunner.cancelTask(
							this.scheduledProcessingTaskId
					).equals(
							new Boolean(
									true
							)
					)
				)
				{
					logger.debug(
							"executeScheduledProcessing - cancelled"
					);
					this.scheduledProcessingTaskId = null;
				}
			}
			task.execute();
		}
	}

	public void executeScheduledProcessing()
	{
		logger.debug(
				"executeScheduledProcessing"
		);
		this.scheduledProcessingTaskId = null;
		for (Map.Entry<Integer, Task> entry : this.transactions.entrySet())
		{
			this.processTransaction(
					entry.getKey()
			);
		}
		if (
			this.transactions.isEmpty() == false
		)
		{
			this.scheduleProcessing();
		}
	}

	public void scheduleProcessing()
	{
		logger.debug(
				"executeScheduledProcessing - scheduling"
		);
		this.scheduledProcessingTaskId = this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						executeScheduledProcessing();
					}
				},
				new Long(
						3 * 1000
				)
		);
	}

	public void processTransaction(
			Integer transactionId
	)
	{
		if (
			this.transactions.containsKey(
					transactionId
			) == false
		)
		{
			return;
		}
		logger.debug(
				this.taskRunner.name + ":getting change"
		);
		UnifiedAction change = this.transactionAnalyzer.getChange(
				transactionId
		);
		if (
			change != null
		)
		{
			if (
				change.componentChange != null
			)
			{
				logger.debug(
						this.taskRunner.name + ":changeComponent, componentId: " + change.componentChange.componentId + " CURRENT: "
								+ change.componentChange.currentVersion + " NEXT: "
								+ change.componentChange.nextVersion
				);
				this.storageGateway.changeComponent(
						change.componentChange,
						this.createNewComponentVersionHandler(
								change.componentChange.transactionId,
								change.componentChange.componentId
						)
				);
			}
			if (
				change.transactionChange != null
			)
			{
				logger.debug(
						this.taskRunner.name + ":changeTransaction, transactionId: " + change.transactionChange.transactionId + " CURRENT: "
								+ change.transactionChange.currentVersion + " NEXT: "
								+ change.transactionChange.nextVersion
				);
				this.storageGateway.changeTransaction(
						change.transactionChange,
						this.createNewTransactionVersionHandler(
								change.transactionChange.transactionId
						)
				);
			}
			if (
				change.downloadRequest != null
			)
			{
				logger.debug(
						this.taskRunner.name + ":downloadRequest" + " transactionId: " + change.downloadRequest.transactionId
								+ " componentId: " + change.downloadRequest.componentId
				);
				assert change.downloadRequest.transactionId != null;
				this.storageGateway.requestTransaction(
						change.downloadRequest.transactionId,
						this.createNewTransactionVersionHandler(
								change.downloadRequest.transactionId
						)
				);
				if (
					change.downloadRequest.componentId != null
				)
				{
					this.storageGateway.requestComponent(
							change.downloadRequest.componentId,
							this.createNewComponentVersionHandler(
									change.downloadRequest.transactionId,
									change.downloadRequest.componentId
							)
					);
				}
			}
		}
		else
		{
			logger.debug(
					this.taskRunner.name + ":getChange - null"
			);
		}
	}

	private NewComponentVersionFeedback createNewComponentVersionHandler(
			Integer transactionId, Integer componentId
	)
	{
		return new NewComponentVersionFeedback()
		{
			@Override
			public void success(
					Component component
			)
			{
				if (
					storage.putComponent(
							componentId,
							component
					)
				)
				{
					processTransaction(
							transactionId
					);
				}
			}

			@Override
			public void failure(
					ErrorCode errorCode
			)
			{
				assert errorCode != ErrorCode.NOT_EXISTS;
			}
		};
	}

	private NewTransactionVersionFeedback createNewTransactionVersionHandler(
			Integer transactionId
	)
	{
		return new NewTransactionVersionFeedback()
		{
			@Override
			public void success(
					Transaction transaction
			)
			{
				if (
					storage.putTransaction(
							transactionId,
							transaction
					)
				)
				{
					processTransaction(
							transactionId
					);
				}
			}

			@Override
			public void failure(
					ErrorCode errorCode
			)
			{
				if (
					errorCode == ErrorCode.NOT_EXISTS
				)
				{
					removeTransaction(
							transactionId
					);
				}
			}
		};
	}

	TaskRunner taskRunner;
	TransactionAnalyzer transactionAnalyzer;
	LocalStorage storage;
	RemoteStorageGateway storageGateway;
	Map<Integer, Task> transactions;
	Integer scheduledProcessingTaskId;
	static final Logger logger = LogManager.getLogger();
}
