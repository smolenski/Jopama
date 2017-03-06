package pl.rodia.mpf;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.rodia.jopama.stats.AsyncOperationsCounters;
import pl.rodia.jopama.stats.StatsResult;
import pl.rodia.jopama.stats.StatsSyncSource;

public class TaskRunner implements Runnable, StatsSyncSource
{

	class ExtendedTask
	{
		public ExtendedTask(
				Task task, Integer taskId
		)
		{
			super();
			this.task = task;
			this.taskId = taskId;
			this.stackTrace = null;
			/*
			 * this.stackTrace = Thread.currentThread().getStackTrace();
			 */
		}

		Task task;
		Integer taskId;
		StackTraceElement[] stackTrace;
	}

	public TaskRunner(
			String name
	)
	{
		super();
		this.name = name;
		this.nextTaskId = new Integer(
				0
		);
		this.finish = new Boolean(
				false
		);
		this.regularTasks = new ArrayList<ExtendedTask>();
		this.timeTasks = new TreeMap<Long, List<ExtendedTask>>();
		this.tasksCounters = new AsyncOperationsCounters(
				this.name + "::tasksCounters"
		);
	}

	public void dumpScheduledTasksTraces()
	{
		logger.debug(
				"WAITING STACK TRACES START"
		);
		for (ExtendedTask extendedTask : this.regularTasks)
		{
			if (
				extendedTask.stackTrace != null
			)
			{

				logger.debug(
						"STACK TRACE START"
				);
				for (StackTraceElement ste : extendedTask.stackTrace)
				{
					System.out.println(
							ste
					);
				}
				logger.debug(
						"STACK TRACE END"
				);
			}

		}
		for (Map.Entry<Long, List<ExtendedTask>> entry : this.timeTasks.entrySet())
		{
			for (ExtendedTask extendedTask : entry.getValue())
			{
				if (
					extendedTask.stackTrace != null
				)
				{
					logger.debug(
							"STACK TRACE START"
					);
					for (StackTraceElement ste : extendedTask.stackTrace)
					{
						System.out.println(
								ste
						);
					}
					logger.debug(
							"STACK TRACE END"
					);
				}
			}
		}
		logger.debug(
				"WAITING STACK TRACES END"
		);
	}

	@Override
	public void run()
	{
		while (
			true
		)
		{
			List<ExtendedTask> tasksToExecute = new ArrayList<ExtendedTask>();
			synchronized (this)
			{
				if (
					this.finish.equals(
							new Boolean(
									true
							)
					) && this.regularTasks.size() == 0 && this.timeTasks.size() == 0
				)
				{
					break;
				}
				if (
					this.finish.equals(
							new Boolean(
									true
							)
					)
				)
				{
					logger.debug(
							"Should finish but cannot, numTasks:" + this.regularTasks.size() + " numTimeTasks: " + this.timeTasks.size()
					);
					this.dumpScheduledTasksTraces();
				}
				Boolean timeTaskPresent = this.timeTasks.size() > 0;
				Boolean timeTaskPresentAndReady = timeTaskPresent && this.timeTasks.firstKey() <= System.currentTimeMillis();
				Boolean taskPresent = this.regularTasks.size() > 0;
				if (
					timeTaskPresentAndReady
				)
				{
					tasksToExecute.addAll(
							this.timeTasks.remove(
									this.timeTasks.firstKey()
							)
					);
				}
				if (
					taskPresent
				)
				{
					tasksToExecute.add(
							this.regularTasks.remove(
									0
							)
					);
				}
				if (
					tasksToExecute.size() == 0
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
			for (ExtendedTask task : tasksToExecute)
			{
				Long startTime = System.currentTimeMillis();
				this.tasksCounters.onRequestStarted();
				task.task.execute();
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
			this.finish = new Boolean(
					true
			);
			this.notify();
		}
	}

	public Integer schedule(
			Task task
	)
	{
		Integer taskId = new Integer(
				++this.nextTaskId
		);
		ExtendedTask extendedTask = new ExtendedTask(
				task,
				taskId
		);
		synchronized (this)
		{
			assert this.finish.equals(
					new Boolean(
							false
					)
			);
			this.regularTasks.add(
					extendedTask
			);
			this.notify();
		}
		return taskId;
	}

	public Integer schedule(
			Task task, Long executionDelayMillis
	)
	{
		Integer taskId = new Integer(
				++this.nextTaskId
		);
		ExtendedTask extendedTask = new ExtendedTask(
				task,
				taskId
		);
		synchronized (this)
		{
			assert this.finish.equals(
					new Boolean(
							false
					)
			);
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
						new LinkedList<ExtendedTask>()
				);
			}
			this.timeTasks.get(
					executionMillis
			).add(
					extendedTask
			);
			this.notify();
		}
		return taskId;
	}

	synchronized public Boolean cancelTask(
			Integer taskId
	)
	{
		for (int i = 0; i < this.regularTasks.size(); ++i)
		{
			ExtendedTask extendedTask = this.regularTasks.get(
					i
			);
			if (
				extendedTask.taskId.equals(
						taskId
				)
			)
			{
				this.regularTasks.remove(
						i
				);
				this.notifyAll();
				return new Boolean(
						true
				);
			}
		}
		for (Map.Entry<Long, List<ExtendedTask>> entry : this.timeTasks.entrySet())
		{
			for (int i = 0; i < entry.getValue().size(); ++i)
			{
				ExtendedTask extendedTask = entry.getValue().get(
						i
				);
				if (
					extendedTask.taskId.equals(
							taskId
					)
				)
				{
					entry.getValue().remove(
							i
					);
					if (
						entry.getValue().size() == 0
					)
					{
						this.timeTasks.remove(
								entry.getKey()
						);
					}
					this.notifyAll();
					return new Boolean(
							true
					);
				}
			}
		}
		return new Boolean(
				false
		);
	}

	@Override
	public StatsResult getStats()
	{
		return this.tasksCounters.getStats();
	}

	public final String name;
	Integer nextTaskId;
	Boolean finish;
	List<ExtendedTask> regularTasks;
	SortedMap<Long, List<ExtendedTask>> timeTasks;
	AsyncOperationsCounters tasksCounters;
	static final Logger logger = LogManager.getLogger();
}
