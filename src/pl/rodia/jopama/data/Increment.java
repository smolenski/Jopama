package pl.rodia.jopama.data;

import java.util.HashMap;
import java.util.Map;

public class Increment extends Function {

	@Override
	public Map<Integer, Integer> execute(Map<Integer, Integer> oldValues) {
		Map<Integer, Integer> result = new HashMap<Integer, Integer>();
		for (Map.Entry<Integer, Integer> entry : oldValues.entrySet())
		{
			result.put(entry.getKey(), entry.getValue() + 1);
		}
		return result;
	}
}
