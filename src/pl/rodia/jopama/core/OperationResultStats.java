package pl.rodia.jopama.core;

import pl.rodia.jopama.stats.StatsResult;
import pl.rodia.jopama.stats.StatsSyncSource;

public class OperationResultStats implements StatsSyncSource
{

	public OperationResultStats(String prefix)
	{
		super();
		this.requestComponentResult = new SuccessFailureCounter(prefix + "::requestComponentResult");
		this.requestTransactionResult = new SuccessFailureCounter(prefix + "::requestTransactionResult");
		this.updateComponentResult = new SuccessFailureCounter(prefix + "::updateComponentResult");
		this.updateTransactionResult = new SuccessFailureCounter(prefix + "::updateTransactionResult");
	}
	
	@Override
	public StatsResult getStats()
	{
		StatsResult result = new StatsResult();
		result.addSamples(this.requestComponentResult.getStats());
		result.addSamples(this.requestTransactionResult.getStats());
		result.addSamples(this.updateComponentResult.getStats());
		result.addSamples(this.updateTransactionResult.getStats());
		return result;
	}

	public SuccessFailureCounter requestComponentResult;
	public SuccessFailureCounter requestTransactionResult;
	public SuccessFailureCounter updateComponentResult;
	public SuccessFailureCounter updateTransactionResult;
}
