package pl.rodia.jopama.integration.zookeeper;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;

import pl.rodia.jopama.data.Component;
import pl.rodia.mpf.Task;

public class ZooKeeperComponentCreator extends ZooKeeperActorBase
{

	public ZooKeeperComponentCreator(
			String addresses, Integer clusterSize, Long firstComponentId, Long numComponents
	)
	{
		super(
				addresses,
				clusterSize
		);
		this.numFilesCreated = new Long(
				0
		);
		this.firstComponentId = firstComponentId;
		this.numComponents = numComponents;
		this.done = new Boolean(
				false
		);
	}

	@Override
	public Long getRetryDelay()
	{
		return new Long(
				3000
		);
	}

	@Override
	public void tryToPerform()
	{
		logger.info("tryToPerform");
		for (long i = this.firstComponentId; i < this.firstComponentId + this.numComponents; ++i)
		{

			ZooKeeperObjectId zooKeeperObjectId = new ZooKeeperObjectId(
					String.format(
							"Component_%020d",
							i
					)
			);
			ZooKeeperProvider zooKeeperProvider = this.zooKeeperMultiProvider.getResponsibleProvider(
					zooKeeperObjectId.getClusterId(
							this.zooKeeperMultiProvider.getNumClusters()
					)
			);
			synchronized (zooKeeperProvider)
			{
				if (
					zooKeeperProvider.zooKeeper == null
							||
							zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
				)
				{
					continue;
				}
				else
				{
					Component component = new Component(new Integer(0), null, new Integer(0), null);
					byte[] serializedComponent = ZooKeeperHelpers.serializeComponent(component);
					zooKeeperProvider.zooKeeper.create(
							ZooKeeperHelpers.getComponentPath(zooKeeperObjectId),
							serializedComponent,
							Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT,
							new StringCallback()
							{
								@Override
								public void processResult(
										int rc, String path, Object ctx, String name
								)
								{
									logger.debug(
											"ZooKeeperCompoentCreator create file process result, name: " + zooKeeperObjectId.uniqueName + " rc: " + rc
									);
									schedule(
											new Task()
											{

												@Override
												public void execute()
												{
													fileCreationFinished(
															rc == KeeperException.Code.OK.intValue()
													);
												}
											}
									);
								}
							},
							zooKeeperObjectId
					);
				}
			}
		}
	}

	void fileCreationFinished(
			Boolean success
	)
	{
		if (
			success.equals(
					new Boolean(
							true
					)
			)
		)
		{
			this.numFilesCreated = this.numFilesCreated + 1;
			if (
				this.numFilesCreated == this.numComponents
			)
			{
				synchronized (this)
				{
					this.done = new Boolean(
							true
					);
				}
			}
		}
	}

	void waitUntilDone() throws InterruptedException
	{
		synchronized (this)
		{
			while (
				this.done.equals(
						new Boolean(
								false
						)
				)
			)
			{
				this.wait(
						new Long(
								1000
						)
				);
			}
		}
	}

	Long numFilesCreated;
	Long firstComponentId;
	Long numComponents;
	Boolean done;

	public static void main(
			String[] args
	)
	{
		assert (args.length == 4);
		String addresses = args[0];
		Integer clusterSize = Integer.parseInt(
				args[1]
		);
		Long firstComponentId = Long.parseLong(
				args[2]
		);
		Long numComponents = Long.parseLong(
				args[3]
		);
		ZooKeeperComponentCreator componentCreator = new ZooKeeperComponentCreator(
				addresses,
				clusterSize,
				firstComponentId,
				numComponents
		);
		componentCreator.start();
		try
		{
			componentCreator.waitUntilDone();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		try
		{
			componentCreator.finish();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}
}
