package pl.rodia.jopama.integration.zookeeper;

import java.util.List;

public interface DirChangesObserver
{
	public void directoryContentChanged(List<String> fileNames);
}
