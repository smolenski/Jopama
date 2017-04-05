package pl.rodia.jopama.data;

import java.io.Serializable;
import java.util.Map;

public abstract class Function implements Serializable {
	public abstract Map<ObjectId, Integer> execute(Map<ObjectId, Integer> oldValues);
	private static final long serialVersionUID = 4467385817857757570L;
}
