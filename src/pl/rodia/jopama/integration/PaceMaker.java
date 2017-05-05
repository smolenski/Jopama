package pl.rodia.jopama.integration;

import java.util.concurrent.ExecutionException;

public interface PaceMaker
{
	public void start();
	public void prepareToFinish() throws InterruptedException, ExecutionException;
	public void finish() throws InterruptedException, ExecutionException;
	Integer getNumFinished();
}