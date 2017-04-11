package pl.rodia.jopama.integration.zookeeper;

import java.util.Random;

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
		logger.debug(
				"args.length: " + args.length
		);
		if (
			args.length != 3
		)
		{
			throw new IllegalArgumentException(
					"Unexpected number of arguments: " + args.length
			);
		}

		ZooKeeperCreator zooKeeperCreator = new ZooKeeperCreator(
				args[0],
				Integer.parseInt(
						args[1]
				)
		);
		zooKeeperCreator.start();
		Integer numComponents = Integer.parseInt(
				args[2]
		);
		Component component = new Component(
				new Integer(
						0
				),
				null,
				new Integer(
						0
				),
				null
		);
		Long time = System.currentTimeMillis();
		Random random = new Random(time);
		Integer base = random.nextInt();
		if (base < 0)
		{
			base = -base;
		}
		for (int i = 0; i < numComponents; ++i)
		{
			logger.info(
					"Creating component: " + i
			);
			ZooKeeperObjectId result = zooKeeperCreator.createObject(
					new ZooKeeperObjectId(
							String.format(
									"Component_%010d_%010d",
									base,
									i
							)
					),
					ZooKeeperHelpers.serializeComponent(
							component
					)
			);
			logger.info(
					"Component created: " + result
			);
		}
		zooKeeperCreator.stop();
	}

	static final Logger logger = LogManager.getLogger();
}
