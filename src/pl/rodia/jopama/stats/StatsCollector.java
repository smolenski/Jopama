package pl.rodia.jopama.stats;

import java.util.List;

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
		this.stopRequested = false;
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

	public void teardown() throws InterruptedException
	{
		this.stopRequested = true;
		this.taskRunner.finish();
		this.taskRunnerThread.join();
	}

	public void scheduleTick()
	{
		this.taskRunner.schedule(
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
		if (this.stopRequested == false)
		{
			this.scheduleTick();
		}
	}

	Boolean stopRequested;
	TaskRunner taskRunner;
	Thread taskRunnerThread;
	List<StatsAsyncSource> sources;
	static final Logger logger = LogManager.getLogger();
}
