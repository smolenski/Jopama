package pl.rodia.jopama.data;

import java.io.Serializable;
import java.util.Map;

public abstract class Function implements Serializable {
	public abstract Map<Integer, Integer> execute(Map<Integer, Integer> oldValues);
	private static final long serialVersionUID = 4467385817857757570L;
}
