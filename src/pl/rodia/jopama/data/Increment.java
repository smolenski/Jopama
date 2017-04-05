package pl.rodia.jopama.data;

import java.util.HashMap;
import java.util.Map;

public class Increment extends Function {

	@Override
	public Map<ObjectId, Integer> execute(Map<ObjectId, Integer> oldValues) {
		Map<ObjectId, Integer> result = new HashMap<ObjectId, Integer>();
		for (Map.Entry<ObjectId, Integer> entry : oldValues.entrySet())
		{
			result.put(entry.getKey(), entry.getValue() + 1);
		}
		return result;
	}
	
	private static final long serialVersionUID = -670456172404239220L;
}
