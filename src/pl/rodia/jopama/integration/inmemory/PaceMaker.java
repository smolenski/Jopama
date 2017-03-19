package pl.rodia.jopama.integration.inmemory;

import java.util.concurrent.ExecutionException;

public interface PaceMaker
{
	public void start();
	public void prepareToFinish() throws InterruptedException, ExecutionException;
	public void finish() throws InterruptedException, ExecutionException;
	Integer getNumFinished();
}
