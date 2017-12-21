package pl.rodia.jopama.stats;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class StatsCollector
{
	public StatsCollector(
			List<StatsAsyncSource> sources
	)
	{
		this.scheduledPeriodicTaskId = null;
		this.stopRequested = new Boolean(false);
		this.taskRunner = new TaskRunner(
				"StatsCollector"
		);
		this.taskRunnerThread = new Thread(
				this.taskRunner
		);
		this.sources = sources;
	}

	public void start()
	{
		this.taskRunnerThread.start();
		this.scheduleTick();
	}
	
	public void prepareToFinish() throws InterruptedException, ExecutionException
	{
		CompletableFuture<Boolean> done = new CompletableFuture<Boolean>();
		this.taskRunner.schedule(new Task()
		{
			@Override
			public void execute()
			{
				stopRequested = new Boolean(true);
				if (scheduledPeriodicTaskId != null)
				{
					if (taskRunner.cancelTask(scheduledPeriodicTaskId).equals(new Boolean(true)))
					{
						scheduledPeriodicTaskId = null;
					}
				}
				done.complete(new Boolean(true));
			}
		});
		Boolean result = done.get();
		assert result.equals(new Boolean(true));
	}
	
	public void finish() throws InterruptedException
	{
		this.taskRunner.finish();
		this.taskRunnerThread.join();
	}

	public void scheduleTick()
	{
		this.scheduledPeriodicTaskId = this.taskRunner.schedule(
				new Task()
				{

					@Override
					public void execute()
					{
						tick();
					}
				},
				new Long(10000)
		);
	}

	public void tick()
	{
		this.scheduledPeriodicTaskId = null;
		for (StatsAsyncSource statsSource : this.sources)
		{
			statsSource.scheduleGetStats(
					this.taskRunner,
					new StatsFeedback()
					{
						@Override
						public void success(
								Long timestamp,
								StatsResult result
						)
						{
							logger.info(
									result
							);
						}
					}
			);
		}
		if (this.stopRequested.equals(new Boolean(false)))
		{
			this.scheduleTick();
		}
	}

	Integer scheduledPeriodicTaskId;
	Boolean stopRequested;
	TaskRunner taskRunner;
	Thread taskRunnerThread;
	List<StatsAsyncSource> sources;
	static final Logger logger = LogManager.getLogger();
	
}
