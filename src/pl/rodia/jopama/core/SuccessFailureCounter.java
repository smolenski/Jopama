package pl.rodia.jopama.core;

import pl.rodia.jopama.stats.OperationCounter;
import pl.rodia.jopama.stats.StatsResult;
import pl.rodia.jopama.stats.StatsSyncSource;

public class SuccessFailureCounter implements StatsSyncSource
{
	public SuccessFailureCounter(String prefix)
	{
		this.successes = new OperationCounter(prefix + "::successes");
		this.failures = new OperationCounter(prefix + "::failures");
	}
	
	public void noticeSuccess()
	{
		this.successes.increase();
	}
	
	public void noticeFailure()
	{
		this.failures.increase();
	}

	@Override
	public StatsResult getStats()
	{
		StatsResult result = new StatsResult();
		result.addSamples(this.successes.getStats());
		result.addSamples(this.failures.getStats());
		return result;
	}

	OperationCounter successes;
	OperationCounter failures;
}
