package pl.rodia.mpf;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import pl.rodia.jopama.stats.AsyncOperationsCounters;
import pl.rodia.jopama.stats.StatsResult;
import pl.rodia.jopama.stats.StatsSyncSource;

public class TaskRunner implements Runnable, StatsSyncSource
{

	public TaskRunner(
			String name
	)
	{
		super();
		this.name = name;
		this.finish = new Boolean(false);
		this.tasks = new ArrayList<Task>();
		this.timeTasks = new TreeMap<Long, List<Task>>();
		this.tasksCounters = new AsyncOperationsCounters(
				this.name + "::tasksCounters"
		);
	}

	@Override
	public void run()
	{
		while (
			!(this.finish.equals(new Boolean(true)) && this.tasks.size() == 0 && this.timeTasks.size() == 0)
		)
		{
			List<Task> tasks = new ArrayList<Task>();
			synchronized (this)
			{
				Boolean timeTaskPresent = this.timeTasks.size() > 0;
				Boolean timeTaskPresentAndReady = timeTaskPresent && this.timeTasks.firstKey() <= System.currentTimeMillis();
				Boolean taskPresent = this.tasks.size() > 0;
				if (
					timeTaskPresentAndReady
				)
				{
					tasks.addAll(
							this.timeTasks.remove(
									this.timeTasks.firstKey()
							)
					);
				}
				if (
					taskPresent
				)
				{
					tasks.add(
							this.tasks.remove(
									0
							)
					);
				}
				if (
					tasks.size() == 0
				) // Waiting
				{
					if (
						timeTaskPresent
					)
					{
						try
						{
							this.wait(
									this.timeTasks.firstKey() - System.currentTimeMillis()
							);
						}
						catch (InterruptedException e)
						{
						}
					}
					else
					{
						try
						{
							this.wait();
						}
						catch (InterruptedException e)
						{
						}
					}
				}
			}
			for (Task task : tasks)
			{
				Long startTime = System.currentTimeMillis();
				this.tasksCounters.onRequestStarted();
				task.execute();
				this.tasksCounters.onRequestFinished(
						System.currentTimeMillis() - startTime
				);
			}
		}
	}

	public void finish()
	{
		synchronized (this)
		{
			this.finish = new Boolean(true);
			this.notify();
		}
	}

	public void schedule(
			Task task
	)
	{
		synchronized (this)
		{
			assert this.finish.equals(new Boolean(false));
			this.tasks.add(
					task
			);
			this.notify();
		}
	}

	public void schedule(
			Task task, Long executionDelayMillis
	)
	{
		synchronized (this)
		{
			assert this.finish.equals(new Boolean(false));
			Long executionMillis = new Long(
					System.currentTimeMillis() + executionDelayMillis
			);
			if (
				this.timeTasks.get(
						executionMillis
				) == null
			)
			{
				this.timeTasks.put(
						executionMillis,
						new LinkedList<Task>()
				);
			}
			this.timeTasks.get(
					executionMillis
			).add(
					task
			);
			this.notify();
		}
	}
	
	@Override
	public StatsResult getStats()
	{
		return this.tasksCounters.getStats();
	}

	public final String name;
	Boolean finish;
	List<Task> tasks;
	SortedMap<Long, List<Task>> timeTasks;
	AsyncOperationsCounters tasksCounters;
}
