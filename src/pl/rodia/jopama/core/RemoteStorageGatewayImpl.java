package pl.rodia.jopama.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.TransactionChange;
import pl.rodia.jopama.gateway.ErrorCode;
import pl.rodia.jopama.gateway.NewComponentVersionFeedback;
import pl.rodia.jopama.gateway.NewTransactionVersionFeedback;
import pl.rodia.jopama.gateway.RemoteStorageGateway;
import pl.rodia.jopama.stats.StatsResult;
import pl.rodia.jopama.stats.StatsSyncSource;
import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class RemoteStorageGatewayImpl extends RemoteStorageGateway implements StatsSyncSource
{

	public RemoteStorageGatewayImpl(
			TaskRunner taskRunner,
			RemoteStorageGateway targetStorageGateway
	)
	{
		super();
		this.taskRunner = taskRunner;
		this.targetStorageGateway = targetStorageGateway;
		this.operationsStats = new OperationStats(
				this.taskRunner.name + "::RemoteStorageGateway"
		);
	}

	@Override
	public void requestTransaction(
			ObjectId transactionId, NewTransactionVersionFeedback feedback
	)
	{
		Long startTime = System.currentTimeMillis();
		this.operationsStats.requestTransaction.onRequestStarted();
		this.targetStorageGateway.requestTransaction(
				transactionId,
				new NewTransactionVersionFeedback()
				{
					@Override
					public void success(
							ExtendedTransaction extendedTransaction
					)
					{
						taskRunner.schedule(
								new Task()
								{
									@Override
									public void execute()
									{
										Long finishTime = System.currentTimeMillis();
										operationsStats.requestTransaction.onRequestFinished(
												finishTime - startTime
										);
										feedback.success(
												extendedTransaction
										);
									}
								}
						);
					}

					@Override
					public void failure(
							ErrorCode errorCode
					)
					{
						taskRunner.schedule(
								new Task()
								{

									@Override
									public void execute()
									{
										Long finishTime = System.currentTimeMillis();
										operationsStats.requestTransaction.onRequestFinished(
												finishTime - startTime
										);
										feedback.failure(
												errorCode
										);
									}
								}
						);
					}
				}
		);
	}

	@Override
	public void requestComponent(
			ObjectId componentId, NewComponentVersionFeedback feedback
	)
	{
		Long startTime = System.currentTimeMillis();
		this.operationsStats.requestComponent.onRequestStarted();
		this.targetStorageGateway.requestComponent(
				componentId,
				new NewComponentVersionFeedback()
				{

					@Override
					public void success(
							ExtendedComponent extendedComponent
					)
					{
						taskRunner.schedule(
								new Task()
								{

									@Override
									public void execute()
									{
										Long finishTime = System.currentTimeMillis();
										operationsStats.requestComponent.onRequestFinished(
												finishTime - startTime
										);
										feedback.success(
												extendedComponent
										);
									}
								}
						);
					}

					@Override
					public void failure(
							ErrorCode errorCode
					)
					{
						taskRunner.schedule(
								new Task()
								{

									@Override
									public void execute()
									{
										Long finishTime = System.currentTimeMillis();
										operationsStats.requestComponent.onRequestFinished(
												finishTime - startTime
										);
										feedback.failure(
												errorCode
										);
									}
								}
						);
					}
				}
		);
	}

	@Override
	public void changeTransaction(
			TransactionChange transactionChange, NewTransactionVersionFeedback feedback
	)
	{
		Long startTime = System.currentTimeMillis();
		this.operationsStats.updateTransaction.onRequestStarted();
		this.targetStorageGateway.changeTransaction(
				transactionChange,
				new NewTransactionVersionFeedback()
				{
					@Override
					public void success(
							ExtendedTransaction extendedTransaction
					)
					{
						if (
							extendedTransaction != null
						)
						{
							logger.debug(
									taskRunner.name + ":transaction updated, transactionId :" + transactionChange.transactionId + ", BASE: "
											+ transactionChange.currentVersion + ", UPDATED: " + transactionChange.nextVersion
							);
							taskRunner.schedule(
									new Task()
									{
										@Override
										public void execute()
										{
											Long finishTime = System.currentTimeMillis();
											operationsStats.updateTransaction.onRequestFinished(
													finishTime - startTime
											);
											feedback.success(
													extendedTransaction
											);
										}
									}
							);
						}
						else
						{

							taskRunner.schedule(
									new Task()
									{
										@Override
										public void execute()
										{
											Long finishTime = System.currentTimeMillis();
											operationsStats.updateTransaction.onRequestFinished(
													finishTime - startTime
											);
											requestTransaction(
													transactionChange.transactionId,
													feedback
											);
										}
									}
							);
						}
					}

					@Override
					public void failure(
							ErrorCode errorCode
					)
					{
						if (
							errorCode == ErrorCode.BASE_VERSION_NOT_EQUAL
						)
						{
							taskRunner.schedule(
									new Task()
									{

										@Override
										public void execute()
										{
											Long finishTime = System.currentTimeMillis();
											operationsStats.updateTransaction.onRequestFinished(
													finishTime - startTime
											);
											requestTransaction(
													transactionChange.transactionId,
													feedback
											);
										}
									}
							);
						}
						else
						{
							taskRunner.schedule(
									new Task()
									{

										@Override
										public void execute()
										{
											Long finishTime = System.currentTimeMillis();
											operationsStats.updateTransaction.onRequestFinished(
													finishTime - startTime
											);
											feedback.failure(
													errorCode
											);
										}
									}
							);
						}
					}
				}
		);
	}

	@Override
	public void changeComponent(
			ComponentChange componentChange, NewComponentVersionFeedback feedback
	)
	{
		Long startTime = System.currentTimeMillis();
		this.operationsStats.updateComponent.onRequestStarted();
		this.targetStorageGateway.changeComponent(
				componentChange,
				new NewComponentVersionFeedback()
				{

					@Override
					public void success(
							ExtendedComponent extendedComponent
					)
					{
						logger.debug(
								taskRunner.name + ":component updated, componentId :" + componentChange.componentId + ", BASE: "
										+ componentChange.currentVersion + ", UPDATED: " + componentChange.nextVersion
						);
						if (
							extendedComponent != null
						)
						{
							taskRunner.schedule(
									new Task()
									{
										@Override
										public void execute()
										{
											Long finishTime = System.currentTimeMillis();
											operationsStats.updateComponent.onRequestFinished(
													finishTime - startTime
											);
											feedback.success(
													extendedComponent
											);
										}
									}
							);
						}
						else
						{
							taskRunner.schedule(
									new Task()
									{
										@Override
										public void execute()
										{
											Long finishTime = System.currentTimeMillis();
											operationsStats.updateComponent.onRequestFinished(
													finishTime - startTime
											);
											requestComponent(
													componentChange.componentId,
													feedback
											);
										}
									}
							);
						}
					}

					@Override
					public void failure(
							ErrorCode errorCode
					)
					{
						if (
							errorCode == ErrorCode.BASE_VERSION_NOT_EQUAL
						)
						{
							taskRunner.schedule(
									new Task()
									{
										@Override
										public void execute()
										{
											Long finishTime = System.currentTimeMillis();
											operationsStats.updateComponent.onRequestFinished(
													finishTime - startTime
											);
											requestComponent(
													componentChange.componentId,
													feedback
											);
										}
									}
							);
						}
						else
						{
							taskRunner.schedule(
									new Task()
									{

										@Override
										public void execute()
										{
											Long finishTime = System.currentTimeMillis();
											operationsStats.updateComponent.onRequestFinished(
													finishTime - startTime
											);
											feedback.failure(
													errorCode
											);
										}
									}
							);
						}
					}
				}
		);
	}

	@Override
	public StatsResult getStats()
	{
		return this.operationsStats.getStats();
	}

	TaskRunner taskRunner;
	RemoteStorageGateway targetStorageGateway;
	OperationStats operationsStats;
	static final Logger logger = LogManager.getLogger();

}
