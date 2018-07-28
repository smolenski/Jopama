package pl.rodia.jopama.integration.zookeeper;

import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class ZooKeeperTransactionCreatorHelperUnitTests
{

	@Test
	public void checkEmpty()
	{
		SortedMap<Long, Long> compCount = new TreeMap<Long, Long>();
		Long compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 1, 1, 10, compCount);
		Assert.assertTrue(
			"checkEmpty 1 component",
			compId.equals(new Long(100))
		);
	}

	@Test
	public void checkBelowLimit()
	{
		SortedMap<Long, Long> compCount = new TreeMap<Long, Long>();
		compCount.put(new Long(100), new Long(1));
		Long compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 1, 2, 10, compCount);
		Assert.assertTrue(
			"checkBelowLimit 1 component",
			compId.equals(new Long(100))
		);
	}

	@Test
	public void checkAtLimit()
	{
		SortedMap<Long, Long> compCount = new TreeMap<Long, Long>();
		compCount.put(new Long(100), new Long(2));
		compCount.put(new Long(101), new Long(1));
		Long compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 2, 2, 10, compCount);
		Assert.assertTrue(
			"checkAtLimit 1 component",
			compId.equals(new Long(101))
		);
	}
	
	@Test
	public void checkBefore()
	{
		SortedMap<Long, Long> compCount = new TreeMap<Long, Long>();
		compCount.put(new Long(101), new Long(2));
		compCount.put(new Long(102), new Long(2));
		Long compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 3, 2, 10, compCount);
		Assert.assertTrue(
			"checkBefore",
			compId.equals(new Long(100))
		);
	}

	@Test
	public void checkMid()
	{
		SortedMap<Long, Long> compCount = new TreeMap<Long, Long>();
		compCount.put(new Long(100), new Long(2));
		compCount.put(new Long(102), new Long(2));
		Long compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 3, 2, 10, compCount);
		Assert.assertTrue(
			"checkMid",
			compId.equals(new Long(101))
		);
	}

	@Test
	public void checkAfter()
	{
		SortedMap<Long, Long> compCount = new TreeMap<Long, Long>();
		compCount.put(new Long(100), new Long(2));
		compCount.put(new Long(101), new Long(2));
		Long compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 3, 2, 10, compCount);
		Assert.assertTrue(
			"checkAfter",
			compId.equals(new Long(102))
		);
	}	
	
	@Test
	public void checkComplex()
	{
		SortedMap<Long, Long> compCount = new TreeMap<Long, Long>();
		Set<Long> compIds = new TreeSet<Long>();
		Long compId;
		compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 3, 2, 10, compCount);
		Assert.assertTrue(
			"checkComplex 101 - first",
			compId.equals(new Long(101))
		);
		compIds.clear();
		compIds.add(compId);
		ZooKeeperTransactionCreatorHelpers.updateCompCount(compCount, compIds);
		compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 3, 2, 9, compCount);
		Assert.assertTrue(
				"checkComplex 100 - first",
				compId.equals(new Long(100))
		);
		compIds.clear();
		compIds.add(compId);
		ZooKeeperTransactionCreatorHelpers.updateCompCount(compCount, compIds);
		compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 3, 2, 31, compCount);
		Assert.assertTrue(
				"checkComplex 101 - second",
				compId.equals(new Long(101))
		);
		compIds.clear();
		compIds.add(compId);
		ZooKeeperTransactionCreatorHelpers.updateCompCount(compCount, compIds);
		compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 3, 2, 21, compCount);
		Assert.assertTrue(
				"checkComplex 102 - first",
				compId.equals(new Long(102))
		);		
		compIds.clear();
		compIds.add(compId);
		ZooKeeperTransactionCreatorHelpers.updateCompCount(compCount, compIds);
		compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 3, 2, 4, compCount);
		Assert.assertTrue(
				"checkComplex 100 - second",
				compId.equals(new Long(100))
		);		
		compIds.clear();
		compIds.add(compId);
		ZooKeeperTransactionCreatorHelpers.updateCompCount(compCount, compIds);
		compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(100, 3, 2, 75, compCount);
		Assert.assertTrue(
				"checkComplex 102 - second",
				compId.equals(new Long(102))
		);
		compIds.clear();
		compIds.add(compId);
		ZooKeeperTransactionCreatorHelpers.updateCompCount(compCount, compIds);
	}
	
	@Test
	public void checkReal()
	{
		SortedMap<Long, Long> compCount = new TreeMap<Long, Long>();
		Set<Long> compIds = new TreeSet<Long>();
		Long compId = null;
		final Long firstComp = new Long(100);
		final Long numComps = new Long(100);
		final Long singleCompLimit = new Long(3);
		for (long i = 0; i < singleCompLimit * numComps; ++i)
		{
			compId = ZooKeeperTransactionCreatorHelpers.generateComponentId(firstComp, numComps, singleCompLimit, i * i, compCount);
			compIds.clear();
			compIds.add(compId);
			ZooKeeperTransactionCreatorHelpers.updateCompCount(compCount, compIds);
		}
		Assert.assertTrue(
				"checkReal - size",
				new Long(compCount.size()).equals(numComps)
		);
		for (long i = firstComp; i < firstComp + numComps; ++i)
		{
			Long num = compCount.get(new Long(i));
			Assert.assertTrue(
					"checkReal, num comps, compId: " + i + " num: " + num,
					singleCompLimit.equals(num)
			);
		}
	}
	
	static final Logger logger = LogManager.getLogger();
}
