package pl.rodia.jopama.stats;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class StatsResult
{
	public StatsResult()
	{
		super();
		this.samples = new TreeMap<String, Double>();
	}

	public void addSample(
			String name, Double value
	)
	{
		this.samples.put(
				name,
				new Double(value)
		);
	}

	public void addSamples(
			StatsResult subResult
	)
	{
		for (Map.Entry<String, Double> entry : subResult.samples.entrySet())
		{
			assert(!this.samples.containsKey(entry.getKey()));
			this.samples.put(
					entry.getKey(),
					new Double(entry.getValue())
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
		for (Map.Entry<String, Double> entry : this.samples.entrySet())
		{
			builder.append(
					entry.getKey() + "=" + String.format("%.2f", entry.getValue()) + ", "
			);
		}
		builder.append(
				")"
		);
		return builder.toString();
	}

	SortedMap<String, Double> samples;
}