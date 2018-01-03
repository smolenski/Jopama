package pl.rodia.jopama.stats;

public class OperationCounter implements StatsSyncSource
{

	public OperationCounter(
		String name
	)
	{
		super();
		this.name = name;
		this.periodStartTime = new Long(System.currentTimeMillis());
		this.numTotal = new Long(0);
		this.numTotalSaved = new Long(0);
	}
	
	public void increase(Long value)
	{
		this.numTotal += value;
	}
	
	public void increase()
	{
		this.increase(new Long(1));
	}
	
	@Override
	public StatsResult getStats()
	{
		Long currentTime = new Long(System.currentTimeMillis());
		Long periodDurationMSec = new Long(currentTime - this.periodStartTime);
		StatsResult result = new StatsResult();
		result.addSample(name + "::total", new Double(this.numTotal));
		if (periodDurationMSec.equals(new Long(0)))
		{
			result.addSample(name + "::diff", new Double(0));	
		}
		else
		{
			result.addSample(name + "::diff", new Double(this.numTotal - this.numTotalSaved) / (new Double(periodDurationMSec) / 1000));
		}
		this.periodStartTime = new Long(System.currentTimeMillis());
		this.numTotalSaved = new Long(this.numTotal);
		return result;
	}

	String name;
	Long periodStartTime;
	Long numTotal;
	Long numTotalSaved;
}
