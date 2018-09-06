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
import pl.rodia.jopama.stats.OperationCounter;
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
		this.operationResultStats = new OperationResultStats(
			this.taskRunner.name + "::RemoteStorageGateway::OperationResult"
		);
		this.operationStats = new OperationStats(
				this.taskRunner.name + "::RemoteStorageGateway::Operation"
		);
		this.componentFailureBaseVersionNotEqual = new OperationCounter("componentFailureBaseVersionNotEqual");
		this.componentFailureStorageNotAvailable = new OperationCounter("componentFailureStorageNotAvailable");
		this.componentFailureEntryNotExists = new OperationCounter("componentFailureEntryNotExists");
		this.transactionFailureBaseVersionNotEqual = new OperationCounter("transactionFailureBaseVersionNotEqual");
		this.transactionFailureStorageNotAvailable = new OperationCounter("transactionFailureStorageNotAvailable");
		this.transactionFailureEntryNotExists = new OperationCounter("transactionFailureEntryNotExists");
	}

	@Override
	public void requestTransaction(
			ObjectId transactionId, NewTransactionVersionFeedback feedback
	)
	{
		Long startTime = System.currentTimeMillis();
		this.operationStats.total.onRequestStarted();
		this.operationStats.requestTransaction.onRequestStarted();
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
										operationResultStats.requestTransactionResult.noticeSuccess();
										Long finishTime = System.currentTimeMillis();
										operationStats.requestTransaction.onRequestFinished(
												finishTime - startTime
										);
										operationStats.total.onRequestFinished(
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
										operationResultStats.requestTransactionResult.noticeFailure();
										Long finishTime = System.currentTimeMillis();
										operationStats.requestTransaction.onRequestFinished(
												finishTime - startTime
										);
										operationStats.total.onRequestFinished(
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
		this.operationStats.total.onRequestStarted();
		this.operationStats.requestComponent.onRequestStarted();
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
										operationResultStats.requestComponentResult.noticeSuccess();
										Long finishTime = System.currentTimeMillis();
										operationStats.requestComponent.onRequestFinished(
												finishTime - startTime
										);
										operationStats.total.onRequestFinished(
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
										operationResultStats.requestComponentResult.noticeFailure();
										Long finishTime = System.currentTimeMillis();
										operationStats.requestComponent.onRequestFinished(
												finishTime - startTime
										);
										operationStats.total.onRequestFinished(
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
		this.operationStats.total.onRequestStarted();
		this.operationStats.updateTransaction.onRequestStarted();
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
											operationResultStats.updateTransactionResult.noticeSuccess();
											Long finishTime = System.currentTimeMillis();
											operationStats.updateTransaction.onRequestFinished(
													finishTime - startTime
											);
											operationStats.total.onRequestFinished(
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
											operationResultStats.updateTransactionResult.noticeSuccess();
											Long finishTime = System.currentTimeMillis();
											operationStats.updateTransaction.onRequestFinished(
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
											operationResultStats.updateTransactionResult.noticeFailure();
											transactionFailureBaseVersionNotEqual.increase();
											Long finishTime = System.currentTimeMillis();
											operationStats.updateTransaction.onRequestFinished(
													finishTime - startTime
											);
											operationStats.total.onRequestFinished(
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
											operationResultStats.updateTransactionResult.noticeFailure();
											if (errorCode == ErrorCode.NOT_AVAILABLE)
											{
												transactionFailureStorageNotAvailable.increase();
											}
											else
											{
												assert(errorCode == ErrorCode.NOT_EXISTS);
												transactionFailureEntryNotExists.increase();
											}
											Long finishTime = System.currentTimeMillis();
											operationStats.updateTransaction.onRequestFinished(
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
		this.operationStats.total.onRequestStarted();
		this.operationStats.updateComponent.onRequestStarted();
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
											operationResultStats.updateComponentResult.noticeSuccess();
											Long finishTime = System.currentTimeMillis();
											operationStats.updateComponent.onRequestFinished(
													finishTime - startTime
											);
											operationStats.total.onRequestFinished(
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
											operationResultStats.updateComponentResult.noticeSuccess();
											Long finishTime = System.currentTimeMillis();
											operationStats.updateComponent.onRequestFinished(
													finishTime - startTime
											);
											operationStats.total.onRequestFinished(
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
											operationResultStats.updateComponentResult.noticeFailure();
											componentFailureBaseVersionNotEqual.increase();
											Long finishTime = System.currentTimeMillis();
											operationStats.updateComponent.onRequestFinished(
													finishTime - startTime
											);
											operationStats.total.onRequestFinished(
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
											operationResultStats.updateComponentResult.noticeFailure();
											if (errorCode == ErrorCode.NOT_AVAILABLE)
											{
												componentFailureStorageNotAvailable.increase();
											}
											else
											{
												assert(errorCode == ErrorCode.NOT_EXISTS);
												componentFailureEntryNotExists.increase();
											}
											Long finishTime = System.currentTimeMillis();
											operationStats.updateComponent.onRequestFinished(
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
		StatsResult result = new StatsResult();
		result.addSamples(this.operationResultStats.getStats());
		result.addSamples(this.operationStats.getStats());
		result.addSamples(this.componentFailureBaseVersionNotEqual.getStats());
		result.addSamples(this.componentFailureStorageNotAvailable.getStats());
		result.addSamples(this.componentFailureEntryNotExists.getStats());
		result.addSamples(this.transactionFailureBaseVersionNotEqual.getStats());
		result.addSamples(this.transactionFailureStorageNotAvailable.getStats());
		result.addSamples(this.transactionFailureEntryNotExists.getStats());
		return result;
	}

	TaskRunner taskRunner;
	RemoteStorageGateway targetStorageGateway;
	OperationStats operationStats;
	OperationResultStats operationResultStats;
	OperationCounter componentFailureBaseVersionNotEqual;
	OperationCounter componentFailureStorageNotAvailable;
	OperationCounter componentFailureEntryNotExists;
	OperationCounter transactionFailureBaseVersionNotEqual;
	OperationCounter transactionFailureStorageNotAvailable;
	OperationCounter transactionFailureEntryNotExists;
	static final Logger logger = LogManager.getLogger();

}
