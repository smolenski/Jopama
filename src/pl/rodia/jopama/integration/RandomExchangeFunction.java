package pl.rodia.jopama.integration;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.data.Function;
import pl.rodia.jopama.data.ObjectId;

public class RandomExchangeFunction extends Function implements Serializable
{

	public RandomExchangeFunction(
			Long transactionId,
			Long seed
	)
	{
		super();
		this.transactionId = transactionId;
		this.seed = seed;
	}

	@Override
	public Map<ObjectId, Integer> execute(
			Map<ObjectId, Integer> oldValues
	)
	{
		Set<Integer> values = new TreeSet<Integer>();
		for (Map.Entry<ObjectId, Integer> entry : oldValues.entrySet())
		{
			values.add(
					entry.getValue()
			);
		}
		if (
			values.size() != oldValues.size()
		)
		{
			logger.debug(
					"VALUES"
			);
			for (Integer val : values)
			{
				logger.debug(
						val
				);
			}
			logger.debug(
					"MAP"
			);
			for (Map.Entry<ObjectId, Integer> entry : oldValues.entrySet())
			{
				logger.debug(
						"key: " + entry.getKey() + " value: " + entry.getValue()
				);
			}
			assert false;
		}
		Random random = new Random(
				this.seed
		);
		Map<ObjectId, Integer> result = new TreeMap<ObjectId, Integer>();
		for (Map.Entry<ObjectId, Integer> entry : oldValues.entrySet())
		{
			result.put(
					entry.getKey(),
					new Integer(
							entry.getValue()
					)
			);
		}
		if (
			result.size() <= 1
		)
		{
			return result;
		}
		for (int i = 0; i < result.size(); ++i)
		{
			int indexToExchangeWith = random.nextInt(
					oldValues.size() - 1
			);
			if (
				indexToExchangeWith >= i
			)
			{
				++indexToExchangeWith;
			}
			assert indexToExchangeWith != i;
			ObjectId keyExchange1 = null;
			ObjectId keyExchange2 = null;
			int vi = 0;
			for (Map.Entry<ObjectId, Integer> entry : result.entrySet())
			{
				if (
					vi == i
				)
				{
					keyExchange1 = entry.getKey();
				}
				if (
					vi == indexToExchangeWith
				)
				{
					keyExchange2 = entry.getKey();
				}
				if (
					keyExchange1 != null && keyExchange2 != null
				)
				{
					break;
				}
				++vi;

			}
			assert keyExchange1 != null;
			assert keyExchange2 != null;
			Integer valueExchange1 = new Integer(
					result.get(
							keyExchange1
					)
			);
			Integer valueExchange2 = new Integer(
					result.get(
							keyExchange2
					)
			);
			result.put(
					keyExchange1,
					valueExchange2
			);
			result.put(
					keyExchange2,
					valueExchange1
			);
		}
		return result;
	}

	private Long transactionId;
	private Long seed;
	private static final long serialVersionUID = -6910284440716455093L;
	static final Logger logger = LogManager.getLogger();

}
