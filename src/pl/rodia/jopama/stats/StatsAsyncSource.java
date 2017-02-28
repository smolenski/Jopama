package pl.rodia.jopama.stats;

import pl.rodia.mpf.Task;
import pl.rodia.mpf.TaskRunner;

public class StatsAsyncSource
{

	public StatsAsyncSource(
			TaskRunner targetRunner, StatsSyncSource targetSource
	)
	{
		super();
		this.targetRunner = targetRunner;
		this.targetSource = targetSource;
	}

	public void scheduleGetStats(
			TaskRunner feedbackRunner,
			StatsFeedback statsFeedback
	)
	{
		this.targetRunner.schedule(
				new Task()
				{

					@Override
					public void execute()
					{
						Long timestamp = System.currentTimeMillis();
						StatsResult result = targetSource.getStats();

						feedbackRunner.schedule(
								new Task()
								{

									@Override
									public void execute()
									{
										statsFeedback.success(
												timestamp,
												result
										);
									}
								}
						);
					}
				}
		);
	}

	public TaskRunner targetRunner;
	public StatsSyncSource targetSource;

}
