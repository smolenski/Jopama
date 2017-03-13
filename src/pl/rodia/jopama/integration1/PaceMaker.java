package pl.rodia.jopama.integration1;

import java.util.concurrent.ExecutionException;

public interface PaceMaker
{
	public void start();
	public void finish() throws InterruptedException, ExecutionException;
}
