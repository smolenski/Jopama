package pl.rodia.jopama.gateway;

import pl.rodia.jopama.data.Component;

public interface NewComponentVersionFeedback
{
	public void success(Component component);
	public void failure(ErrorCode errorCode);
}
