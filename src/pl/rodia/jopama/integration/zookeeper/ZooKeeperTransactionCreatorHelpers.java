package pl.rodia.jopama.integration.zookeeper;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.Transaction;

public class ZooKeeperTransactionCreatorHelpers
{
	
	static public Long getNumUnusableComponents(long singleComponentLimit, Map<Long, Long> compCount)
	{
		long numUnusable = 0;
		for (Map.Entry<Long, Long> entry : compCount.entrySet())
		{
			if (entry.getValue() == singleComponentLimit)
			{
				numUnusable += 1;
			}
		}
		return numUnusable;
	}
	
	static public Long generateComponentId(
		long firstComponentId,
		long numComponents,
		long singleComponentLimit,
		long desiredSeqDisc,
		SortedMap<Long, Long> compCount
	)
	{
		long numUnusable = ZooKeeperTransactionCreatorHelpers.getNumUnusableComponents(singleComponentLimit, compCount).longValue();
		assert (numUnusable < numComponents);
		Long componentSeq = Math.floorMod(
			desiredSeqDisc,
			numComponents - numUnusable
		);
		long lastIncluded = firstComponentId - 1;
		long usableCount = 0;
		long notUsableCount = 0;
		for (Map.Entry<Long, Long> entry : compCount.entrySet())
		{
			Long currId = entry.getKey();
			Long currVal = entry.getValue();
			long usableCountDelta = currId - lastIncluded;
			long notUsableDelta = 0;
			if (currVal.longValue() == singleComponentLimit)
			{
				notUsableDelta = 1;
			}
			usableCountDelta -= notUsableDelta;
			if (usableCount + usableCountDelta > componentSeq)
			{
				break;
			}
			lastIncluded = currId;
			usableCount += usableCountDelta;
			notUsableCount += notUsableDelta;
		}
		long result = firstComponentId + componentSeq + notUsableCount;
		assert (result >= firstComponentId && result < firstComponentId + numComponents);
		return result;
	}
	
	static public Boolean allCompsBelowLimit(
		long singleComponentLimit,
		SortedMap<Long, Long> compsCount,
		Set<Long> compIds
	)
	{
		for (Long compId : compIds)
		{
			Long counter = compsCount.get(compId);
			if (counter != null)
			{
				assert (counter.compareTo(new Long(singleComponentLimit)) <= 0);
				if (counter.equals(new Long(singleComponentLimit)))
				{
					return new Boolean(false);
				}
			}
		}
		return new Boolean(true);
	}
	
	static public Set<Long> getCompIds(Transaction transaction)
	{
		Set<Long> ids = new TreeSet<Long>();
		for (ObjectId compId : transaction.transactionComponents.keySet())
		{
			ZooKeeperObjectId zooCompId = (ZooKeeperObjectId) compId;
			Long cId = zooCompId.getId();
			ids.add(cId);
		}
		return ids;
	}
	
	static public void updateCompCount(Map<Long, Long> compCount, Transaction transaction)
	{
		updateCompCount(compCount, ZooKeeperTransactionCreatorHelpers.getCompIds(transaction));
	}

	static public void updateCompCount(Map<Long, Long> compCount, Set<Long> compIds)
	{
		for (Long cId : compIds)
		{
			if (compCount.containsKey(cId))
			{
				Long num = compCount.get(cId);
				compCount.put(cId, new Long(num.longValue() + 1));
			}
			else
			{
				compCount.put(cId, new Long(1));
			}
		}
	}
		
	
}
