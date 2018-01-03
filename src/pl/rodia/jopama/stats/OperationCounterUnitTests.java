package pl.rodia.jopama.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class OperationCounterUnitTests
{
	@Before
	public void initialize()
	{
		this.TIME_UNIT_SEC = new Double(0.1);
		this.STATS_BASE_NAME = new String(
				"TestCounters"
		);
		this.counter = new OperationCounter(
				STATS_BASE_NAME
		);		
	}
	
	public void assertStatValueEqual(StatsResult statsResult, String name, Double expectedValue, Long tolerancePercent)
	{
		StatsUnitTestsHelpers.assertStatValueEqual("stat" + name, this.getStatValue(statsResult, name), expectedValue, tolerancePercent);
	}
	
	public void assertStatValueEqual(StatsResult statsResult, String name, Double value)
	{
		this.assertStatValueEqual(statsResult, name, value, new Long(10));
	}
	
	public Double getStatValue(StatsResult statsResult, String statName)
	{
		return statsResult.samples.get(
				STATS_BASE_NAME + "::" + statName
		);
	}

	@Test
	public void shouldHandleZeroRequests() throws InterruptedException
	{
		Thread.sleep(
				new Long(Math.round(1000 * this.TIME_UNIT_SEC))
		);
		StatsResult statsResult = counter.getStats();
		this.assertStatValueEqual(statsResult, "total", new Double(0));
		this.assertStatValueEqual(statsResult, "diff", new Double(0));
	}

	@Test
	public void shouldHandleOneRequest() throws InterruptedException
	{
		Thread.sleep(
				new Long(Math.round(1000 * this.TIME_UNIT_SEC))
		);
		counter.increase();
		Thread.sleep(
				new Long(Math.round(1000 * this.TIME_UNIT_SEC))
		);
		StatsResult statsResult = counter.getStats();
		this.assertStatValueEqual(statsResult, "total", new Double(1));
		this.assertStatValueEqual(statsResult, "diff", new Double(1) / (2 * this.TIME_UNIT_SEC));
	}

	@Test
	public void shouldHandleTwoRequests() throws InterruptedException
	{
		counter.increase(new Long(5));
		Thread.sleep(
				new Long(Math.round(1000 * this.TIME_UNIT_SEC))
		);
		counter.increase(new Long(10));
		Thread.sleep(
				new Long(Math.round(1000 * this.TIME_UNIT_SEC))
		);
		StatsResult statsResult = counter.getStats();
		this.assertStatValueEqual(statsResult, "total", new Double(15));
		this.assertStatValueEqual(statsResult, "diff", new Double(15) / (2 * this.TIME_UNIT_SEC));
	}

	@Test
	public void shouldHandleDoubleCollection() throws InterruptedException
	{
		counter.increase(new Long(5));
		Thread.sleep(
				new Long(Math.round(1000 * this.TIME_UNIT_SEC))
		);
		StatsResult statsResult = counter.getStats();
		this.assertStatValueEqual(statsResult, "total", new Double(5));
		this.assertStatValueEqual(statsResult, "diff", new Double(5) / this.TIME_UNIT_SEC);
		counter.increase(new Long(10));
		Thread.sleep(
				new Long(Math.round(1000 * this.TIME_UNIT_SEC))
		);
		statsResult = counter.getStats();
		this.assertStatValueEqual(statsResult, "total", new Double(15));
		this.assertStatValueEqual(statsResult, "diff", new Double(10) / (this.TIME_UNIT_SEC));
	}
	
	Double TIME_UNIT_SEC;
	String STATS_BASE_NAME;
	OperationCounter counter;
	static final Logger logger = LogManager.getLogger();
	
}
