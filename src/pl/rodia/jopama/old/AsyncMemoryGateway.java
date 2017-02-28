package pl.rodia.jopama.old;

import pl.rodia.jopama.LocalStorage;
import pl.rodia.jopama.TransactionProcessor;
import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.TransactionChange;
import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class AsyncMemoryGateway extends Gateway
{

	public AsyncMemoryGateway(
			TaskRunner taskRunner, LocalStorage storage, TransactionProcessor transactionProcessor
	)
	{
		super();
		this.taskRunner = taskRunner;
		this.storage = storage;
		this.transactionProcessor = transactionProcessor;
	}

	@Override
	public void requestTransaction(
			Integer transactionId
	)
	{
		this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						storage.putTransaction(
								transactionId,
								storage.getTransaction(
										transactionId
								)
						);
						transactionProcessor.processTransaction(
								transactionId
						);
					}
				}
		);
	}

	@Override
	public void requestComponent(
			Integer transactionId,
			Integer componentId
	)
	{
		this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						storage.putComponent(
								componentId,
								storage.getComponent(
										componentId
								)
						);
						transactionProcessor.processTransaction(
								transactionId
						);
					}
				}
		);
	}

	@Override
	public void changeTransaction(
			TransactionChange transactionChange
	)
	{
		// TODO Auto-generated method stub
		this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						if (
							transactionChange.nextVersion == null
						)
						{
							transactionProcessor.removeTransaction(
									transactionChange.transactionId
							);
							return;
						}
						if (
							storage.getTransaction(
									transactionChange.transactionId
							) == transactionChange.currentVersion
						)
						{
							storage.putTransaction(
									transactionChange.transactionId,
									transactionChange.nextVersion
							);
						}
						storage.putTransaction(
								transactionChange.transactionId,
								storage.getTransaction(
										transactionChange.transactionId
								)
						);
						transactionProcessor.processTransaction(
								transactionChange.transactionId
						);
					}
				}
		);

	}

	@Override
	public void changeComponent(
			ComponentChange componentChange
	)
	{
		// TODO Auto-generated method stub
		this.taskRunner.schedule(
				new Task()
				{
					@Override
					public void execute()
					{
						if (
							storage.getComponent(
									componentChange.componentId
							) == componentChange.currentVersion
						)
						{
							storage.putComponent(
									componentChange.componentId,
									componentChange.nextVersion
							);
						}
						storage.putComponent(
								componentChange.componentId,
								storage.getComponent(
										componentChange.componentId
								)
						);
						transactionProcessor.processTransaction(
								componentChange.transactionId
						);
					}
				}
		);
	}

	TaskRunner taskRunner;
	LocalStorage storage;
	TransactionProcessor transactionProcessor;
}
