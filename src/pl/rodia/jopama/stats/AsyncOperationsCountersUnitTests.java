package pl.rodia.jopama.stats;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AsyncOperationsCountersUnitTests
{
	
	@Before
	public void initialize()
	{
		this.TIME_UNIT_MS = new Long(100);
		this.STATS_BASE_NAME = new String(
				"TestCounters"
		);
		this.counters = new AsyncOperationsCounters(
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
				new Long(1 * this.TIME_UNIT_MS)
		);
		StatsResult statsResult = counters.getStats();
		this.assertStatValueEqual(statsResult, "numStarted", new Double(0));
		this.assertStatValueEqual(statsResult, "numFinished", new Double(0));
		this.assertStatValueEqual(statsResult, "maxOutstanding", new Double(0));
		this.assertStatValueEqual(statsResult, "totalDuration", new Double(0));
		this.assertStatValueEqual(statsResult, "avgDuration", new Double(0));
		this.assertStatValueEqual(statsResult, "maxDuration", new Double(0));
		this.assertStatValueEqual(statsResult, "avgOutstanding", new Double(0));
		this.assertStatValueEqual(statsResult, "anyOutstanding", new Double(0));
		this.assertStatValueEqual(statsResult, "numFinishedDiff", new Double(0));
		this.assertStatValueEqual(statsResult, "periodDuration", new Double(1 * this.TIME_UNIT_MS));
	}

	@Test
	public void shouldHandleOneRequest() throws InterruptedException
	{
		counters.onRequestStarted();
		Thread.sleep(
				new Long(1 * this.TIME_UNIT_MS)
		);
		counters.onRequestFinished(new Long(1 * this.TIME_UNIT_MS));
		Thread.sleep(
				new Long(1 * this.TIME_UNIT_MS)
		);
		StatsResult statsResult = counters.getStats();
		this.assertStatValueEqual(statsResult, "numStarted", new Double(1));
		this.assertStatValueEqual(statsResult, "numFinished", new Double(1));
		this.assertStatValueEqual(statsResult, "maxOutstanding", new Double(1));
		this.assertStatValueEqual(statsResult, "totalDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "maxDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgOutstanding", new Double(0.5));
		this.assertStatValueEqual(statsResult, "anyOutstanding", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "numFinishedDiff", new Double(1) / (2 * new Double(this.TIME_UNIT_MS) / 1000));
		this.assertStatValueEqual(statsResult, "periodDuration", new Double(2 * this.TIME_UNIT_MS));
	}

	@Test
	public void shouldHandleTwoRequests() throws InterruptedException
	{
		counters.onRequestStarted();
		Thread.sleep(
				new Long(1 * this.TIME_UNIT_MS)
		);
		counters.onRequestFinished(new Long(1 * this.TIME_UNIT_MS));
		counters.onRequestStarted();
		Thread.sleep(
				new Long(1 * this.TIME_UNIT_MS)
		);
		counters.onRequestFinished(new Long(1 * this.TIME_UNIT_MS));
		StatsResult statsResult = counters.getStats();
		this.assertStatValueEqual(statsResult, "numStarted", new Double(2));
		this.assertStatValueEqual(statsResult, "numFinished", new Double(2));
		this.assertStatValueEqual(statsResult, "maxOutstanding", new Double(1));
		this.assertStatValueEqual(statsResult, "totalDuration", new Double(2 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "maxDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgOutstanding", new Double(1));
		this.assertStatValueEqual(statsResult, "anyOutstanding", new Double(2 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "numFinishedDiff", new Double(2) / (2 * new Double(this.TIME_UNIT_MS) / 1000));
		this.assertStatValueEqual(statsResult, "periodDuration", new Double(2 * this.TIME_UNIT_MS));
	}

	@Test
	public void shouldHandleTwoConsurrentRequests() throws InterruptedException
	{
		counters.onRequestStarted();
		counters.onRequestStarted();
		Thread.sleep(
				new Long(1 * this.TIME_UNIT_MS)
		);
		counters.onRequestFinished(new Long(1 * this.TIME_UNIT_MS));
		counters.onRequestFinished(new Long(1 * this.TIME_UNIT_MS));
		Thread.sleep(
				new Long(1 * this.TIME_UNIT_MS)
		);
		StatsResult statsResult = counters.getStats();
		this.assertStatValueEqual(statsResult, "numStarted", new Double(2));
		this.assertStatValueEqual(statsResult, "numFinished", new Double(2));
		this.assertStatValueEqual(statsResult, "maxOutstanding", new Double(2));
		this.assertStatValueEqual(statsResult, "totalDuration", new Double(2 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "maxDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgOutstanding", new Double(1));
		this.assertStatValueEqual(statsResult, "anyOutstanding", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "numFinishedDiff", new Double(2) / (2 * new Double(this.TIME_UNIT_MS) / 1000));
		this.assertStatValueEqual(statsResult, "periodDuration", new Double(2 * this.TIME_UNIT_MS));
	}

	@Test
	public void shouldHandleOneFinishedAndOneOutstandingRequest() throws InterruptedException
	{
		counters.onRequestStarted();
		counters.onRequestStarted();
		Thread.sleep(
				new Long(1 * this.TIME_UNIT_MS)
		);
		counters.onRequestFinished(new Long(1 * this.TIME_UNIT_MS));
		Thread.sleep(
				new Long(1 * this.TIME_UNIT_MS)
		);
		StatsResult statsResult = counters.getStats();
		this.assertStatValueEqual(statsResult, "numStarted", new Double(2));
		this.assertStatValueEqual(statsResult, "numFinished", new Double(1));
		this.assertStatValueEqual(statsResult, "maxOutstanding", new Double(2));
		this.assertStatValueEqual(statsResult, "totalDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "maxDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgOutstanding", new Double(0.5));
		this.assertStatValueEqual(statsResult, "anyOutstanding", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "numFinishedDiff", new Double(1) / (2 * new Double(this.TIME_UNIT_MS) / 1000));
		this.assertStatValueEqual(statsResult, "periodDuration", new Double(2 * this.TIME_UNIT_MS));
		counters.onRequestFinished(new Long(2 * this.TIME_UNIT_MS));
	}
	
	@Test
	public void shouldHandleDoubleStatsCollection() throws InterruptedException
	{
		counters.onRequestStarted();
		Thread.sleep(
				new Long(1 * this.TIME_UNIT_MS)
		);
		counters.onRequestFinished(new Long(1 * this.TIME_UNIT_MS));
		StatsResult statsResult = counters.getStats();
		this.assertStatValueEqual(statsResult, "numStarted", new Double(1));
		this.assertStatValueEqual(statsResult, "numFinished", new Double(1));
		this.assertStatValueEqual(statsResult, "maxOutstanding", new Double(1));
		this.assertStatValueEqual(statsResult, "totalDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "maxDuration", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgOutstanding", new Double(1));
		this.assertStatValueEqual(statsResult, "anyOutstanding", new Double(1 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "numFinishedDiff", new Double(1) / (new Double(this.TIME_UNIT_MS) / 1000));
		this.assertStatValueEqual(statsResult, "periodDuration", new Double(1 * this.TIME_UNIT_MS));
		
		Thread.sleep(
				new Long(1 * this.TIME_UNIT_MS)
		);
		counters.onRequestStarted();
		Thread.sleep(
				new Long(2 * this.TIME_UNIT_MS)
		);
		counters.onRequestFinished(new Long(2 * this.TIME_UNIT_MS));
		statsResult = counters.getStats();
		this.assertStatValueEqual(statsResult, "numStarted", new Double(2));
		this.assertStatValueEqual(statsResult, "numFinished", new Double(2));
		this.assertStatValueEqual(statsResult, "maxOutstanding", new Double(1));
		this.assertStatValueEqual(statsResult, "totalDuration", new Double(2 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgDuration", new Double(2 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "maxDuration", new Double(2 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "avgOutstanding", new Double(0.66));
		this.assertStatValueEqual(statsResult, "anyOutstanding", new Double(2 * this.TIME_UNIT_MS));
		this.assertStatValueEqual(statsResult, "numFinishedDiff", new Double(1) / (3 * new Double(this.TIME_UNIT_MS) / 1000));
		this.assertStatValueEqual(statsResult, "periodDuration", new Double(3 * this.TIME_UNIT_MS));
	}
	
	@Test
	public void shouldHandleOverlappingInRequestAndStatsCollection() throws InterruptedException
	{
		List<StatsResult> results = new LinkedList<StatsResult>();
		for (int i = 0; i < 105; ++i)
		{
			if (i % 7 == 1)
			{
				this.counters.onRequestStarted();
			}
			if (i % 7 == 3)
			{
				this.counters.onRequestStarted();
			}
			if (i % 7 == 4)
			{
				this.counters.onRequestFinished(new Long(1 * this.TIME_UNIT_MS));
			}
			if (i % 7 == 6)
			{
				this.counters.onRequestFinished(new Long(5 * this.TIME_UNIT_MS));
			}
			if (i > 0 && i % 5 == 0)
			{
				results.add(this.counters.getStats());
			}
			Thread.sleep(this.TIME_UNIT_MS);
		}
		results.add(this.counters.getStats());
		StatsResult finalResult = results.get(results.size() - 1);
		this.assertStatValueEqual(finalResult, "numStarted", new Double(30));
		this.assertStatValueEqual(finalResult, "numFinished", new Double(30));
		for (StatsResult singleResult : results)
		{
			Double value = this.getStatValue(singleResult, "maxOutstanding");
			Boolean equalLower = StatsUnitTestsHelpers.isStatValueEqual(value, new Double(1));
			Boolean equalUpper = StatsUnitTestsHelpers.isStatValueEqual(value, new Double(2));
			Assert.assertTrue("stat: maxOutstanding value: " + value, equalLower || equalUpper);
		}
		Double totalDurationSum = new Double(0);
		for (StatsResult singleResult : results)
		{
			totalDurationSum += singleResult.samples.get(STATS_BASE_NAME + "::totalDuration");
		}
		StatsUnitTestsHelpers.assertStatValueEqual("totalDuration sum", totalDurationSum, new Double(90 * this.TIME_UNIT_MS));
		for (StatsResult singleResult : results)
		{
			Double value = this.getStatValue(singleResult, "avgDuration");
			Boolean equalLower = StatsUnitTestsHelpers.isStatValueEqual(value, new Double(1 * this.TIME_UNIT_MS));
			Boolean equalMiddle = StatsUnitTestsHelpers.isStatValueEqual(value, new Double(3 * this.TIME_UNIT_MS));
			Boolean equalUpper = StatsUnitTestsHelpers.isStatValueEqual(value, new Double(5 * this.TIME_UNIT_MS));
			Assert.assertTrue("stat: avgDuration value: " + value, equalLower || equalMiddle || equalUpper);
		}
		for (StatsResult singleResult : results)
		{
			Double value = this.getStatValue(singleResult, "maxDuration");
			Boolean equalLower = StatsUnitTestsHelpers.isStatValueEqual(value, new Double(1 * this.TIME_UNIT_MS));
			Boolean equalUpper = StatsUnitTestsHelpers.isStatValueEqual(value, new Double(5 * this.TIME_UNIT_MS));
			Assert.assertTrue("stat: maxDuration value: " + value, equalLower || equalUpper);
		}
		for (StatsResult singleResult : results)
		{
			Double value = this.getStatValue(singleResult, "avgOutstanding");
			Boolean biggerThanLower = StatsUnitTestsHelpers.isStatValueBigger(value, new Double(0.2));
			Boolean lowerThanUpper = StatsUnitTestsHelpers.isStatValueLower(value, new Double(1.2));
			Assert.assertTrue("stat: avgOutstanding value: " + value, biggerThanLower && lowerThanUpper);
		}
		for (StatsResult singleResult : results)
		{
			Double value = this.getStatValue(singleResult, "anyOutstanding");
			Boolean biggerThanLower = StatsUnitTestsHelpers.isStatValueBigger(value, new Double(1 * this.TIME_UNIT_MS));
			Boolean lowerThanUpper = StatsUnitTestsHelpers.isStatValueLower(value, new Double(5 * this.TIME_UNIT_MS));
			Assert.assertTrue("stat: anyOutstanding value: " + value, biggerThanLower && lowerThanUpper);
		}
		for (StatsResult singleResult : results)
		{
			Double value = this.getStatValue(singleResult, "numFinishedDiff");
			Boolean equalLower = StatsUnitTestsHelpers.isStatValueEqual(value, new Double(1) / (5 * new Double(this.TIME_UNIT_MS) / 1000));
			Boolean equalUpper = StatsUnitTestsHelpers.isStatValueEqual(value, new Double(2) / (5 * new Double(this.TIME_UNIT_MS) / 1000));
			Assert.assertTrue("stat: numFinishedDiff value: " + value, equalLower || equalUpper);
		}
		for (StatsResult singleResult : results)
		{
			Double value = this.getStatValue(singleResult, "periodDuration");
			Assert.assertTrue("stat: periodDuration value: " + value, StatsUnitTestsHelpers.isStatValueEqual(value, new Double(5 * this.TIME_UNIT_MS)));
		}
	}
	
	Long TIME_UNIT_MS;
	String STATS_BASE_NAME;
	AsyncOperationsCounters counters;
	static final Logger logger = LogManager.getLogger();
}
