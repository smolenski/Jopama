package pl.rodia.jopama.integration.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException.OperationTimeoutException;

import pl.rodia.jopama.data.Component;

public class ZooKeeperComponentCreator
{
	public static void main(
			String[] args
	) throws InterruptedException, OperationTimeoutException
	{
		logger.debug("args.length: " + args.length);
		if (
			args.length != 2
		)
		{
			throw new IllegalArgumentException(
					"Unexpected number of arguments: " + args.length
			);
		}

		ZooKeeperCreator zooKeeperCreator = new ZooKeeperCreator(
				"ZooKeeperCreator",
				args[0]
		);
		zooKeeperCreator.start();
		Integer num = Integer.parseInt(
				args[1]
		);
		Component component = new Component(new Integer(0), null, new Integer(0), null);
		for (int i = 0; i < num; ++i)
		{	
			logger.info("Creating component: " + i);
			Integer componentId = new Integer(i);
			if (zooKeeperCreator.createObject(ZooKeeperHelpers.getComponentPath(componentId), ZooKeeperHelpers.serializeComponent(component)).equals(new Boolean(false)))
			{
				throw new OperationTimeoutException();
			}
		}
		zooKeeperCreator.stop();
	}

	static final Logger logger = LogManager.getLogger();
}
