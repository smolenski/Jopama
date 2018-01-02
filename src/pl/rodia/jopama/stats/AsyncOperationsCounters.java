package pl.rodia.jopama.stats;

public class AsyncOperationsCounters implements StatsSyncSource
{

	public AsyncOperationsCounters(String prefix)
	{
		this.prefix = prefix;
		this.numStarted = new Long(0);
		this.numFinishedSaved = new Long(0);
		this.numFinished = new Long(0);
		this.anyOutstandingSyncTimeMs = null;
		this.reset();
	}
	
	public void reset()
	{
		this.numFinishedSaved = new Long(this.numFinished);
		this.maxOutstanding = new Long(0);
		this.totalDuration = new Long(0);
		this.maxDuration = new Long(0);		
		this.anyOutstanding = new Long(0);
		this.resetTimeMs = new Long(System.currentTimeMillis());
	}
	
	@Override
	public StatsResult getStats()
	{
		StatsResult result = new StatsResult();
		result.addSample(this.prefix + "::numStarted", new Double(this.numStarted));
		result.addSample(this.prefix + "::numFinished", new Double(this.numFinished));
		result.addSample(this.prefix + "::maxOutstanding", new Double(this.maxOutstanding));
		result.addSample(this.prefix + "::totalDuration", new Double(this.totalDuration));
		Long numFinishedRecently = new Long(this.numFinished - this.numFinishedSaved);
		if (!numFinishedRecently.equals(new Long(0)))
		{
			result.addSample(this.prefix + "::avgDuration", new Double(Math.round(this.totalDuration / numFinishedRecently)));
		}
		else
		{
			result.addSample(this.prefix + "::avgDuration", new Double(0));
		}
		result.addSample(this.prefix + "::maxDuration", new Double(this.maxDuration));
		Long periodDuration = System.currentTimeMillis() - this.resetTimeMs;
		if (!periodDuration.equals(new Long(0)))
		{
			result.addSample(this.prefix + "::avgOutstanding", new Double(this.totalDuration) / periodDuration);
			result.addSample(this.prefix + "::numFinishedDiff", new Double(numFinishedRecently) / (new Double(periodDuration) / 1000));
		}
		else
		{
			result.addSample(this.prefix + "::avgOutstanding", new Double(0));
			result.addSample(this.prefix + "::numFinishedDiff", new Double(0));
		}
		result.addSample(this.prefix + "::anyOutstanding", new Double(this.anyOutstanding));
		result.addSample(this.prefix + "::periodDuration", new Double(periodDuration));
		this.reset();
		return result;
	}
	
	public void onRequestStarted()
	{
		assert (this.numStarted.compareTo(this.numFinished) >= 0);
		if (this.numStarted.equals(this.numFinished))
		{
			assert (this.anyOutstandingSyncTimeMs == null);
			this.anyOutstandingSyncTimeMs = new Long(System.currentTimeMillis());
		}
		++this.numStarted;
		this.maxOutstanding = Math.max(this.maxOutstanding, this.numStarted - this.numFinished);
	}
	
	public void onRequestFinished(Long durationMs)
	{
		assert (this.numStarted.compareTo(this.numFinished) > 0);
		++this.numFinished;
		this.totalDuration = this.totalDuration + durationMs;
		this.maxDuration = Math.max(this.maxDuration, durationMs);
		Long currentTime = System.currentTimeMillis();
		assert (this.anyOutstandingSyncTimeMs != null);
		this.anyOutstanding = this.anyOutstanding + (currentTime - this.anyOutstandingSyncTimeMs);
		this.anyOutstandingSyncTimeMs = currentTime;
		if (this.numStarted.equals(this.numFinished))
		{
			this.anyOutstandingSyncTimeMs = null;
		}
	}
	
	final String prefix;
	Long numStarted;
	Long numFinishedSaved;
	Long numFinished;
	Long maxOutstanding;
	Long totalDuration;
	Long maxDuration;
	Long anyOutstanding;
	Long resetTimeMs;
	Long anyOutstandingSyncTimeMs;
}
