package pl.rodia.jopama;

import java.util.HashSet;
import java.util.Set;

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
		this.transactionIds = new HashSet<Integer>();
		this.processingScheduled = false;
		this.scheduleProcessing();
	}

	public void addTransaction(
			Integer transactionId
	)
	{
		this.transactionIds.add(
				transactionId
		);
		if (
			this.processingScheduled == false
		)
		{
			this.scheduleProcessing();
		}
		this.processTransaction(
				transactionId
		);
	}

	public void removeTransaction(
			Integer transactionId
	)
	{
		this.transactionIds.remove(
				transactionId
		);
	}

	public void executeScheduledProcessing()
	{
		this.processingScheduled = false;
		for (Integer transactionId : this.transactionIds)
		{
			this.processTransaction(
					transactionId
			);
		}
		if (
			this.transactionIds.isEmpty() == false
		)
		{
			this.scheduleProcessing();
		}
	}

	public void scheduleProcessing()
	{
		this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						executeScheduledProcessing();
					}
				},
				new Long(
						2 * 1000
				)
		);
		this.processingScheduled = true;
	}

	public void processTransaction(
			Integer transactionId
	)
	{
		if (
			this.transactionIds.contains(
					transactionId
			) == false
		)
		{
			return;
		}
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
				System.out.println(this.taskRunner.name + ":changeComponent" + " CURRENT: " + change.componentChange.currentVersion + " NEXT: " + change.componentChange.nextVersion);
				this.storageGateway.changeComponent(
						change.componentChange,
						this.createNewComponentVersionHandler(change.componentChange.transactionId, change.componentChange.componentId)
				);
			}
			if (
				change.transactionChange != null
			)
			{
				System.out.println(this.taskRunner.name + ":changeTransaction" + " CURRENT: " + change.transactionChange.currentVersion + " NEXT: " + change.transactionChange.nextVersion);
				this.storageGateway.changeTransaction(
						change.transactionChange,
						this.createNewTransactionVersionHandler(change.transactionChange.transactionId)
				);
			}
			if (
				change.downloadRequest != null
			)
			{
				System.out.println(this.taskRunner.name + ":downloadRequest" + " transactionId: " + change.downloadRequest.transactionId + " componentId: " + change.downloadRequest.componentId);
				assert change.downloadRequest.transactionId != null;
				this.storageGateway.requestTransaction(
						change.downloadRequest.transactionId,
						this.createNewTransactionVersionHandler(change.downloadRequest.transactionId)
				);
				if (
					change.downloadRequest.componentId != null
				)
				{
					this.storageGateway.requestComponent(
							change.downloadRequest.componentId,
							this.createNewComponentVersionHandler(change.downloadRequest.transactionId, change.downloadRequest.componentId)
					);
				}
			}
		}
		else
		{
			System.out.println(this.taskRunner.name + ":getChange - null");
		}
	}

	private NewComponentVersionFeedback createNewComponentVersionHandler(Integer transactionId, Integer componentId)
	{
		return new NewComponentVersionFeedback()
		{
			@Override
			public void success(
					Component component
					)
			{
				storage.putComponent(componentId, component);
				processTransaction(transactionId);
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

	private NewTransactionVersionFeedback createNewTransactionVersionHandler(Integer transactionId)
	{
		return new NewTransactionVersionFeedback()
		{
			@Override
			public void success(
					Transaction transaction
			)
			{
				storage.putTransaction(
						transactionId,
						transaction
				);
				processTransaction(
						transactionId
				);
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
	Set<Integer> transactionIds;
	boolean processingScheduled;

}
