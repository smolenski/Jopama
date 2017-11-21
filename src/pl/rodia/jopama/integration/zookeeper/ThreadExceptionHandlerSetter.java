package pl.rodia.jopama.integration.zookeeper;

import java.lang.Thread.UncaughtExceptionHandler;
import java.io.StringWriter;
import java.io.PrintWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ThreadExceptionHandlerSetter
{

	public static void setHandler()
	{
		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler()
		{
			@Override
			public void uncaughtException(
					Thread t, Throwable e
			)
			{
                StringWriter errorStr = new StringWriter();
                e.printStackTrace(new PrintWriter(errorStr));
				logger.error(
					"UncaughtException in thread: " + t.toString() + " exception: " + e.toString() + " backtrace: " + errorStr.toString()
				);
				System.exit(1);
			}
		});
	}
	
	static final Logger logger = LogManager.getLogger();
}
