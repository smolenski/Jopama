package pl.rodia.jopama.stats;

import org.junit.Assert;

public class StatsUnitTestsHelpers
{
	
	static public Boolean isStatValueEqual(Double value, Double expectedValue, Long tolerancePercent)
	{
		return 
		StatsUnitTestsHelpers.isStatValueBigger(value, expectedValue, tolerancePercent)
		&&
		StatsUnitTestsHelpers.isStatValueLower(value, expectedValue, tolerancePercent)
		;
	}
	
	static public Boolean isStatValueBigger(Double value, Double expectedValue, Long tolerancePercent)
	{
		assert (tolerancePercent.compareTo(new Long(0)) >= 0);
		assert (tolerancePercent.compareTo(new Long(100)) <= 0);
		return 
		(value.compareTo(expectedValue * (100 - tolerancePercent) / 100) >= 0)
		;
	}
	
	static public Boolean isStatValueLower(Double value, Double expectedValue, Long tolerancePercent)
	{
		assert (tolerancePercent.compareTo(new Long(0)) >= 0);
		assert (tolerancePercent.compareTo(new Long(100)) <= 0);
		return 
		(value.compareTo(expectedValue * (100 + tolerancePercent) / 100) <= 0)
		;
	}
	
	static public Boolean isStatValueEqual(Double value, Double expectedValue)
	{
		return StatsUnitTestsHelpers.isStatValueEqual(value, expectedValue, new Long(10));
	}
	
	static public Boolean isStatValueBigger(Double value, Double expectedValue)
	{
		return StatsUnitTestsHelpers.isStatValueBigger(value, expectedValue, new Long(10));
	}
	
	static public Boolean isStatValueLower(Double value, Double expectedValue)
	{
		return StatsUnitTestsHelpers.isStatValueLower(value, expectedValue, new Long(10));
	}

	static public void assertStatValueEqual(String whatComparingStr, Double value, Double expectedValue, Long tolerancePercent)
	{
		Assert.assertTrue(
				"Comparing " + whatComparingStr + " value: " + value + " expectedValue: " + expectedValue + " tolerancePercent: " + tolerancePercent,
				StatsUnitTestsHelpers.isStatValueEqual(value, expectedValue, tolerancePercent)
		);		
	}

	static public void assertStatValueEqual(String whatComparingStr, Double value, Double expectedValue)
	{
		StatsUnitTestsHelpers.assertStatValueEqual(whatComparingStr, value, expectedValue, new Long(10));
	}
	
}
