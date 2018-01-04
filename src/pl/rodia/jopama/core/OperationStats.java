package pl.rodia.jopama.core;

import pl.rodia.jopama.stats.AsyncOperationsCounters;
import pl.rodia.jopama.stats.OperationCounter;
import pl.rodia.jopama.stats.StatsResult;
import pl.rodia.jopama.stats.StatsSyncSource;

public class OperationStats implements StatsSyncSource
{
	
	public OperationStats(String prefix)
	{
		super();
		this.requestComponent = new AsyncOperationsCounters(prefix + "::requestComponent");
		this.requestTransaction = new AsyncOperationsCounters(prefix + "::requestTransaction");
		this.updateComponent = new AsyncOperationsCounters(prefix + "::updateComponent");
		this.updateTransaction = new AsyncOperationsCounters(prefix + "::updateTransaction");
		this.total = new AsyncOperationsCounters(prefix + "::total");
		this.requestComponentSuccess = new OperationCounter(prefix + "::requestComponentSuccess");
		this.requestComponentFailure = new OperationCounter(prefix + "::requestComponentFailure");
		this.requestTransactionSuccess = new OperationCounter(prefix + "::requestTransactionSuccess");
		this.requestTransactionFailure = new OperationCounter(prefix + "::requestTransactionFailure");
		this.updateComponentSuccess = new OperationCounter(prefix + "::updateComponentSuccess");
		this.updateComponentFailure = new OperationCounter(prefix + "::updateComponentFailure");
		this.updateTransactionSuccess = new OperationCounter(prefix + "::updateTransactionSuccess");
		this.updateTransactionFailure = new OperationCounter(prefix + "::updateTransactionFailure");	
	}
	
	@Override
	public StatsResult getStats()
	{
		StatsResult result = new StatsResult();
		result.addSamples(this.requestComponent.getStats());
		result.addSamples(this.requestTransaction.getStats());
		result.addSamples(this.updateComponent.getStats());
		result.addSamples(this.updateTransaction.getStats());
		result.addSamples(this.total.getStats());
		result.addSamples(this.requestComponentSuccess.getStats());
		result.addSamples(this.requestComponentFailure.getStats());
		result.addSamples(this.requestTransactionSuccess.getStats());
		result.addSamples(this.requestTransactionFailure.getStats());
		result.addSamples(this.updateComponentSuccess.getStats());
		result.addSamples(this.updateComponentFailure.getStats());
		result.addSamples(this.updateTransactionSuccess.getStats());
		result.addSamples(this.updateTransactionFailure.getStats());
		return result;
	}
	
	public AsyncOperationsCounters requestComponent;
	public AsyncOperationsCounters requestTransaction;
	public AsyncOperationsCounters updateComponent;
	public AsyncOperationsCounters updateTransaction;
	public AsyncOperationsCounters total;
	public OperationCounter requestComponentSuccess;
	public OperationCounter requestComponentFailure;
	public OperationCounter requestTransactionSuccess;
	public OperationCounter requestTransactionFailure;
	public OperationCounter updateComponentSuccess;
	public OperationCounter updateComponentFailure;
	public OperationCounter updateTransactionSuccess;
	public OperationCounter updateTransactionFailure;	
}
