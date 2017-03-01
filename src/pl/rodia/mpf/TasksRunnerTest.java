package pl.rodia.mpf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class TasksRunnerTest
{

	public TasksRunnerTest()
	{
		this.taskRunner = new TaskRunner("TestTaskRunner");
		this.taskRunnerThread = new Thread(
				this.taskRunner
		);
	}

	void start()
	{
		TasksRunnerTest.initSeq();
		this.taskRunnerThread.start();
	}

	void teardown() throws InterruptedException
	{
		this.taskRunner.finish();
		this.taskRunnerThread.join();
	}

	@Test
	public void scheduledTaskShouldBeExecuted() throws InterruptedException
	{
		logger.info(
				"Running scheduledTaskShouldBeExecuted"
		);
		this.start();
		ExecChecker execChecker = new ExecChecker(
				"Task1"
		);
		this.taskRunner.schedule(
				execChecker
		);
		execChecker.waitUntilExecuted();
		this.teardown();
	}

	@Test
	public void scheduledTimedTaskShouldBeExecuted() throws InterruptedException
	{
		logger.info(
				"Running scheduledTimedTaskShouldBeExecuted"
		);
		this.start();
		ExecChecker execChecker = new ExecChecker(
				"Task1"
		);
		this.taskRunner.schedule(
				execChecker,
				new Long(
						50
				)
		);
		execChecker.waitUntilExecuted();
		this.teardown();
	}

	@Test
	public void scheduledTasksShouldBeExecuted() throws InterruptedException
	{
		logger.info(
				"Running scheduledTasksShouldBeExecuted"
		);
		this.start();
		ExecChecker execChecker1 = new ExecChecker(
				"Task1"
		);
		ExecChecker execChecker2 = new ExecChecker(
				"Task2"
		);
		ExecChecker execChecker3 = new ExecChecker(
				"Task3"
		);
		ExecChecker execChecker4 = new ExecChecker(
				"Task4"
		);
		this.taskRunner.schedule(
				execChecker1,
				new Long(
						100
				)
		);
		this.taskRunner.schedule(
				execChecker2,
				new Long(
						200
				)
		);
		this.taskRunner.schedule(
				execChecker3
		);
		this.taskRunner.schedule(
				execChecker4
		);
		execChecker1.waitUntilExecuted();
		execChecker2.waitUntilExecuted();
		execChecker3.waitUntilExecuted();
		execChecker4.waitUntilExecuted();
		assertThat(
				execChecker3.order,
				is(
						1
				)
		);
		assertThat(
				execChecker4.order,
				is(
						2
				)
		);
		assertThat(
				execChecker1.order,
				is(
						3
				)
		);
		assertThat(
				execChecker2.order,
				is(
						4
				)
		);
		this.teardown();
	}

	@Test
	public void scheduledTimedTaskShouldBeExecutedInPlannedTimeOrder() throws InterruptedException
	{
		logger.info(
				"Running scheduledTimedTaskShouldBeExecutedInPlannedTimeOrder"
		);
		this.start();
		ExecChecker execChecker1 = new ExecChecker(
				"Task1"
		);
		ExecChecker execChecker2 = new ExecChecker(
				"Task2"
		);
		this.taskRunner.schedule(
				execChecker1,
				new Long(
						100
				)
		);
		this.taskRunner.schedule(
				execChecker2,
				new Long(
						50
				)
		);
		execChecker1.waitUntilExecuted();
		execChecker2.waitUntilExecuted();
		assertThat(
				execChecker2.order,
				is(
						1
				)
		);
		assertThat(
				execChecker1.order,
				is(
						2
				)
		);
		this.teardown();
	}

	class ExecChecker implements Task
	{
		ExecChecker(
				String id
		)
		{
			this.id = id;
			this.executed = false;
			this.order = new Integer(
					0
			);
		}

		public synchronized void execute()
		{
			logger.debug(
					"Executing id: " + this.id
			);
			this.executed = true;
			TasksRunnerTest.incSeq();
			this.order = TasksRunnerTest.seq;
			logger.debug(
					"Executing id: " + this.id + " order: " + this.order
			);
			this.notify();
		}

		public synchronized void waitUntilExecuted()
		{
			while (
				!this.executed
			)
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

		String id;
		Boolean executed;
		Integer order;
	};

	static void incSeq()
	{
		seq += 1;
	}

	static void initSeq()
	{
		seq = 0;
	}

	static int seq;
	TaskRunner taskRunner;
	Thread taskRunnerThread;
	static final Logger logger = LogManager.getLogger();
}
