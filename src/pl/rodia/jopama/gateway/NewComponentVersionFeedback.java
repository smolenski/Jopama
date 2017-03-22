package pl.rodia.jopama.gateway;

import pl.rodia.jopama.data.ExtendedComponent;

public interface NewComponentVersionFeedback
{
	public void success(ExtendedComponent extendedComponent);
	public void failure(ErrorCode errorCode);
}
