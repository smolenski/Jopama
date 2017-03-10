package pl.rodia.jopama.core;

import pl.rodia.jopama.stats.AsyncOperationsCounters;
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
	}
	
	@Override
	public StatsResult getStats()
	{
		StatsResult result = new StatsResult();
		result.addSamples(this.requestComponent.getStats());
		result.addSamples(this.requestTransaction.getStats());
		result.addSamples(this.updateComponent.getStats());
		result.addSamples(this.updateTransaction.getStats());
		return result;
	}
	
	public AsyncOperationsCounters requestComponent;
	public AsyncOperationsCounters requestTransaction;
	public AsyncOperationsCounters updateComponent;
	public AsyncOperationsCounters updateTransaction;
	
}
