package pl.rodia.jopama.stats;

public class AsyncOperationsCounters implements StatsSyncSource
{

	public AsyncOperationsCounters(String prefix)
	{
		this.reset();
		this.prefix = prefix;
	}
	
	public void reset()
	{
		this.numStarted = new Long(0);
		this.numFinished = new Long(0);
		this.maxOutstanding = new Long(0);
		this.totalDuration = new Long(0);
		this.avgDuration = new Long(0);
		this.maxDuration = new Long(0);		
		this.avgOutstanding = new Long(0);
		this.anyOutstanding = new Long(0);
		this.resetTimeMs = System.currentTimeMillis(); 
	}
	
	@Override
	public StatsResult getStats()
	{
		StatsResult result = new StatsResult();
		result.addSample(this.prefix + "::numStarted", this.numStarted);
		result.addSample(this.prefix + "::numFinished", this.numFinished);
		result.addSample(this.prefix + "::maxOutstanding", this.maxOutstanding);
		result.addSample(this.prefix + "::totalDuration", this.totalDuration);
		if (!this.numFinished.equals(new Long(0)))
		{
			this.avgDuration = (long) Math.round(this.totalDuration / this.numFinished);
		}
		else
		{
			this.avgDuration = new Long(0);
		}
		result.addSample(this.prefix + "::avgDuration", this.avgDuration);
		result.addSample(this.prefix + "::maxDuration", this.maxDuration);
		Long periodDuration = System.currentTimeMillis() - this.resetTimeMs;
		if (!periodDuration.equals(new Long(0)))
		{
			result.addSample(this.prefix + "::avgOutstanding", this.totalDuration  / periodDuration);
		}
		else
		{
			result.addSample(this.prefix + "::avgOutstanding", null);
		}
		result.addSample(this.prefix + "::anyOutstanding", this.anyOutstanding);
		result.addSample(this.prefix + "::periodDuration", periodDuration);
		this.reset();
		return result;
	}
	
	public void onRequestStarted()
	{
		if (this.numStarted.equals(this.numFinished))
		{
			this.firstOutstandingTimeMs = System.currentTimeMillis();
		}
		++this.numStarted;
		this.maxOutstanding = Math.max(this.maxOutstanding, this.numStarted - this.numFinished);
	}
	
	public void onRequestFinished(Long durationMs)
	{
		++this.numFinished;
		this.totalDuration = this.totalDuration + durationMs;
		this.maxDuration = Math.max(this.maxDuration, durationMs);
		if (this.numStarted.equals(this.numFinished))
		{
			this.anyOutstanding = this.anyOutstanding + (System.currentTimeMillis() - this.firstOutstandingTimeMs);
		}
	}
	
	final String prefix;
	Long numStarted;
	Long numFinished;
	Long maxOutstanding;
	Long totalDuration;
	Long avgDuration;
	Long maxDuration;
	Long avgOutstanding;
	Long anyOutstanding;
	Long resetTimeMs;
	Long firstOutstandingTimeMs;
}
