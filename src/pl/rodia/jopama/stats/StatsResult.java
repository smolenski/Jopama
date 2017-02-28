package pl.rodia.jopama.stats;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class StatsResult
{
	public StatsResult()
	{
		super();
		this.samples = new TreeMap<String, Long>();
	}

	public void addSample(
			String name, Long value
	)
	{
		this.samples.put(
				name,
				new Long(value)
		);
	}

	public void addSamples(
			StatsResult subResult
	)
	{
		for (Map.Entry<String, Long> entry : subResult.samples.entrySet())
		{
			this.samples.put(
					entry.getKey(),
					new Long(entry.getValue())
			);
		}
	}

	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		builder.append(
				"("
		);
		for (Map.Entry<String, Long> entry : this.samples.entrySet())
		{
			builder.append(
					entry.getKey() + "=" + entry.getValue() + ", "
			);
		}
		builder.append(
				")"
		);
		return builder.toString();
	}

	SortedMap<String, Long> samples;
}
